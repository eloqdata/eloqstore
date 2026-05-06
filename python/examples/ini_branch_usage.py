from pathlib import Path
import tempfile

from eloqstore import Client, Options


def main() -> None:
    root = Path(tempfile.mkdtemp(prefix="eloqstore-py-ini-"))
    store_path = root / "data"
    store_path.mkdir(parents=True, exist_ok=True)

    ini_path = root / "eloqstore.ini"
    ini_path.write_text(
        "[run]\n"
        "num_threads = 1\n"
        "buffer_pool_size = 4MB\n\n"
        "[permanent]\n"
        "data_page_size = 4KB\n",
        encoding="utf-8",
    )

    client = Client(
        Options(
            store_paths=[str(store_path)],
            options_path=str(ini_path),
            table_name="demo",
            partition_id=0,
            branch="feature-x",
            term=7,
            partition_group_id=3,
        )
    )
    try:
        client.put("branch-key", b"value")
        print("branch read:", client.get("branch-key"))
    finally:
        client.close()


if __name__ == "__main__":
    main()
