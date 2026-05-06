from pathlib import Path
import tempfile

from eloqstore import Client, Options


def main() -> None:
    root = Path(tempfile.mkdtemp(prefix="eloqstore-py-example-"))
    store_path = root / "data"
    store_path.mkdir(parents=True, exist_ok=True)

    first = Client(
        Options(
            store_paths=[str(store_path)],
            table_name="demo",
            partition_id=0,
            num_threads=1,
        )
    )
    try:
        first.put("persist", b"value")
        print("first read:", first.get("persist"))
    finally:
        first.close()

    reopened = Client(
        Options(
            store_paths=[str(store_path)],
            table_name="demo",
            partition_id=0,
            num_threads=1,
        )
    )
    try:
        print("reopened read:", reopened.get("persist"))
    finally:
        reopened.close()


if __name__ == "__main__":
    main()
