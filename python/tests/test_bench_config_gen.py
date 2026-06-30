from eloqstore.bench_config_gen import PRESETS, build_conversations


def test_build_conversations_shape():
    conversations = build_conversations(conversations=2, first_turn_repeats=3)

    assert [conv["id"] for conv in conversations] == ["conv_0", "conv_1"]
    assert [len(conv["messages"]) for conv in conversations] == [4, 4]
    assert conversations[0]["messages"][0]["role"] == "user"
    assert "Conversation 0:" in conversations[0]["messages"][0]["content"]


def test_benchmark_presets_match_documented_conversation_counts():
    assert PRESETS["fit-5g"].conversations == 3
    assert PRESETS["overflow-5g"].conversations == 7
    assert PRESETS["high-hit"].conversations == 64
