from __future__ import annotations

from pathlib import Path
import websocket
import json
from typing import Generator, Callable
import logging
import time
import threading
from uuid import uuid4 as uid4
import sys
from typing import NamedTuple
import functools
import requests
import pandas as pd

logging.root.setLevel(logging.WARNING)

root = logging.getLogger()

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.WARNING)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
root.addHandler(handler)


class Sender:
    ws: websocket.WebSocket
    work_queue: Callable[[], Generator[dict, None, None]]
    logger: logging.Logger
    stop: bool = False

    def __init__(
        self,
        work_queue: Callable[[], Generator[dict, None, None]],
    ):
        self.ws = None
        self.work_queue = work_queue
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

    def run(self):
        for message in self.work_queue():
            if self.stop:
                break
            msg = json.dumps(message)
            self.logger.info(f"sending message {msg}")
            self.ws.send(msg)

    def run_concurrent(self, ws) -> threading.Thread:
        self.ws = ws
        t = threading.Thread(target=self.run, daemon=True)
        self.logger.info("starting sender thread")
        t.start()
        return t


class Receiver:
    ws: websocket.WebSocket
    ok_handler: Callable[[str], None]
    retry_handler: Callable[[str], None]
    logger: logging.Logger
    stop: bool = False

    def __init__(
        self,
        ws: websocket.WebSocket,
        retry_handler: Callable[[str], None],
        ok_handler: Callable[[str], None],
    ):
        self.ws = ws
        self.retry_handler = retry_handler
        self.ok_handler = ok_handler
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.WARNING)

    def on_msg(self, ws, msg):
        self.logger.info(f"Got msg {msg}")
        decoded = json.loads(msg)
        if not (uid := decoded.get("msg", {}).get("uid")):
            self.logger.warning(f"could not find uid in {decoded}")
            return
        if not decoded.get("OK"):
            self.logger.warning(f"retrying failed msg {decoded=}")
            self.retry_handler(uid)
        else:
            self.logger.info(f"message acknowledged ok: {uid=}")
            self.ok_handler(uid)

    def on_error(self, ws, error):
        self.logger.error(error)


class Client:
    sender: Sender
    sender_thread: threading.Thread | None
    receiver: Receiver
    receiver_thread: threading.Thread | None
    logger: logging.Logger
    pending: dict[str, dict]
    sent: dict[str, dict]
    ok: list[dict]
    socket: websocket.WebSocketApp
    retry_tracker: dict[str, int]

    def __init__(self):
        self.retry_tracker = {}
        self.ok = []
        self.pending = {}
        self.sent = {}
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        self.socket = None
        self.host = "ws://localhost:8080/ws"
        self.sender = Sender(self.work_queue)
        self.sender_thread = None
        self.receiver = Receiver(self.socket, self.retry_handler, self.ok_handler)

    def enqueue(self, count: int = 1):
        for uid in (f"{uid4()}" for _ in range(count)):
            self.pending[uid] = {"uid": uid}

    def enqueue_num(self, count: int = 1):
        for uid in (f"{x}" for x in range(count)):
            self.retry_tracker[uid] = 0
            self.pending[uid] = {"uid": uid}

    def work_queue(self) -> Generator[dict, None, None]:
        while not self.receiver.stop:
            self.logger.info(f"{len(self.pending)=}")
            self.logger.info(f"{len(self.sent)=}")
            self.logger.info(f"{len(self.ok)=}")
            if not self.pending and self.sent:
                self.logger.info(
                    f"All sent, awaiting {len(self.sent)} responses, 1 sec..."
                )
                time.sleep(0.1)
                continue
            elif not self.pending and not self.sent:
                self.receiver.stop = True
                self.sender.stop = True
                break
            next_msg_uid = [*self.pending.keys()][0]
            next_msg = self.pending.pop(next_msg_uid)
            self.sent[next_msg_uid] = next_msg
            yield next_msg

        self.logger.info("Sending Complete")
        self.socket.close(status=websocket.STATUS_NORMAL)

    def ok_handler(self, uid: str) -> None:
        if uid not in self.pending and uid not in self.sent:
            self.logger.warning(f"unrecognised msg id {uid=}")
            return

        if uid in self.pending:
            self.logger.warning(f"received unexpected ok {uid=}")
            msg = self.pending.pop(uid)
            self.ok.append(msg)
            return

        msg = self.sent.pop(uid)
        self.logger.info(f"Message ok {msg=}")
        self.ok.append(msg)
        self.retry_tracker[uid] = self.retry_tracker.get(uid, 0) + 1

    def retry_handler(self, uid: str) -> None:
        if uid not in self.pending and uid not in self.sent:
            self.logger.warning(f"unrecognised msg id {uid=}")
            return

        if uid in self.pending:
            self.logger.warning(f"received unexpected fail {uid=}")
            self.logger.info(f"keeping {uid=}")
            return

        self.retry_tracker[uid] = self.retry_tracker.get(uid, 0) + 1

        msg = self.sent.pop(uid)
        self.logger.info(f"Message retrying {msg=}")
        self.pending[uid] = msg

    def stop(self, *args):
        self.sender.stop = True
        self.receiver.stop = True
        self.sender_thread.join()

    def on_open(self, ws):
        self.socket = ws
        self.sender_thread = self.sender.run_concurrent(ws)

    def report_retries(self, output_fn: Callable[[str], None]) -> list[ReportEntry]:
        msg_count = len(self.retry_tracker)
        tot = sum(self.retry_tracker.values())
        avg = sum(self.retry_tracker.values()) / msg_count
        top_ten = sorted(
            self.retry_tracker.items(), key=lambda item: item[1], reverse=True
        )[:10]
        max_retries = top_ten[0][1]

        success_rate_with_n_retries = (
            lambda n: len(
                [*filter(lambda item: item[1] <= n, self.retry_tracker.items())]
            )
            / msg_count
        )

        success_rates = []
        success_rate_data = []

        for n in range(1, max_retries + 1):
            percent_rate = success_rate_with_n_retries(n) * 100
            success_rate_data.append((n, percent_rate))
            success_rates.append(f"{n}\t{percent_rate:.2f}%")

        success_rates = ["retries\tsuccess rate"] + [*reversed(success_rates)]
        rpt_rates = "\n".join(success_rates)

        report = (
            "RETRY REPORT\n"
            "============\n\n"
            f"Message Count: {msg_count}\n"
            f"Total attempts: {tot}\n"
            f"max_retries: {max_retries}\n\n"
            f"Average attempts per uid: {avg}\n\n"
            f"{rpt_rates}\n\n"
        )

        output_fn(report)
        return [
            ReportEntry(
                message_count=msg_count,
                total_attempts=tot,
                max_retries_to_complete=max_retries,
                avg_attempt_count=avg,
                actual_success_rate=actual_success_rate,
                retry_count=n,
                measured_success_rate=success_rate_with_n_retries(n),
            )
            for n in range(1, 17)
        ]


class ReportEntry(NamedTuple):
    message_count: int
    total_attempts: int
    max_retries_to_complete: int
    avg_attempt_count: float
    actual_success_rate: float
    retry_count: int
    measured_success_rate: float


class Retry(NamedTuple):
    uid: str
    count: int


initial_success_rate = 80.0
increment_rate = 2.0
actual_success_rate = initial_success_rate


def set_success_rate(rate: float):
    global actual_success_rate
    if requests.get(f"http://localhost:8080/?rate={rate:.2f}").status_code != 200:
        raise Exception("Couldn't set rate")
    else:
        actual_success_rate = rate


def run(count):
    client = Client()
    client.enqueue_num(count)
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(
        client.host,
        on_message=client.receiver.on_msg,
        on_error=client.receiver.on_error,
        on_close=client.stop,
    )
    ws.on_open = client.on_open
    ws.run_forever()
    result = client.report_retries(print)
    del ws
    del client
    return result


class Params(NamedTuple):
    success_rate: float
    batch_size: int


def param_space() -> Generator[Params, None, None]:
    while actual_success_rate < 100.0:
        set_success_rate(actual_success_rate + increment_rate)
        for count in (1500,):
            yield Params(success_rate=actual_success_rate, batch_size=count)


def main():
    results = []
    for params in param_space():
        results.append(run(params.batch_size))
        time.sleep(1)
    return results


if __name__ == "__main__":
    results = main()

    report_path = Path.cwd() / "report.csv"
    if report_path.exists():
        df = pd.read_csv(report_path)
        for res in functools.reduce(lambda a, b: a + b, results, []):
            df.loc[len(df)] = res._asdict()
    else:
        data = {
            f: [
                getattr(r, f) for r in functools.reduce(lambda a, b: a + b, results, [])
            ]
            for f in ReportEntry._fields
        }
        df = pd.DataFrame(data=data)

    csv = df.to_csv(index=False)
    report_path.write_text(csv)
