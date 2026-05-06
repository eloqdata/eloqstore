from eloqstore import Client, Options


def main() -> None:
    client = Client(Options(table_name="demo", partition_id=0, num_threads=1))
    try:
        client.put("hello", b"world")
        print("exists:", client.exists("hello"))
        print("value:", client.get("hello"))

        client.batch_put({"k1": b"v1", "k2": b"v2"})
        print("batch:", client.batch_get(["k1", "k2", "missing"]))
    finally:
        client.close()


if __name__ == "__main__":
    main()
