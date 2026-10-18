from telemetry.bus.memory import MemoryBus


def test_same_key_same_partition_in_order():
    bus = MemoryBus()
    p = bus.producer()
    for i in range(20):
        p.send("t", "device-a", str(i).encode())
    recs = bus.consumer("g", ["t"]).poll_batch(100)
    assert len({r.partition for r in recs}) == 1
    assert [int(r.value) for r in recs] == list(range(20))


def test_uncommitted_records_are_redelivered_after_restart():
    bus = MemoryBus()
    p = bus.producer()
    for i in range(10):
        p.send("t", f"k{i}", b"x")
    c = bus.consumer("g", ["t"])
    first = c.poll_batch(100)
    assert len(first) == 10 and c.lag() == 10
    c.rewind_to_committed()  # crash before commit
    again = c.poll_batch(100)
    assert len(again) == 10
    c.commit(again)
    assert c.lag() == 0
    assert bus.consumer("g", ["t"]).poll_batch(100) == []
    assert len(bus.consumer("other-group", ["t"]).poll_batch(100)) == 10
