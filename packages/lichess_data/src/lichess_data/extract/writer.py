from compression import zstd
import chess.pgn
import pyarrow as pa
import pyarrow.parquet as pq


from libs.shared import load_config

cfg = load_config("lichess_data")
level = cfg.get("logging", {}).get("level", "INFO")


INPUT = cfg.get("extract", {}).get("input")
OUTPUT = cfg.get("extract", {}).get("output")

print(f"Input: {INPUT}"
      f"\nOutput: {OUTPUT}")

BATCH_SIZE = cfg.get("extract", {}).get("batch_size")

print(f"Batch size: {BATCH_SIZE}")

def safe_int(x):
    if x is None:
        return None
    if x == "?" or x == "":
        return None
    try:
        return int(x)
    except:
        return None


def parse_game(game):
    headers = game.headers

    moves = []
    node = game

    while not node.is_end():
        node = node.variation(0)
        if node.move:
            moves.append(node.move.uci())

    return {
        "event": headers.get("Event"),
        "site": headers.get("Site"),
        "date": headers.get("Date"),
        "round": headers.get("Round"),

        "white": headers.get("White"),
        "black": headers.get("Black"),

        "result": headers.get("Result"),

        "utc_date": headers.get("UTCDate"),
        "utc_time": headers.get("UTCTime"),
        "white_elo": safe_int(headers.get("WhiteElo")),
        "black_elo": safe_int(headers.get("BlackElo")),
        "white_rating_diff": safe_int(headers.get("WhiteRatingDiff")),
        "black_rating_diff": safe_int(headers.get("BlackRatingDiff")),
        "eco": headers.get("ECO"),
        "opening": headers.get("Opening"),

        "time_control": headers.get("TimeControl"),
        "termination": headers.get("Termination"),
        "moves": moves,
    }


def write_parquet_stream(input_path, output_path):
    batch = []
    writer = None
    total = 0

    with zstd.open(input_path, "rt", encoding="utf-8") as f:
        while True:
            game = chess.pgn.read_game(f)
            if game is None:
                break

            batch.append(parse_game(game))
            total += 1

            # write batch
            if len(batch) >= BATCH_SIZE:
                table = pa.Table.from_pylist(batch)

                if writer is None:
                    writer = pq.ParquetWriter(output_path, table.schema)

                writer.write_table(table)
                batch.clear()

        # flush remaining
        if batch:
            table = pa.Table.from_pylist(batch)

            if writer is None:
                writer = pq.ParquetWriter(output_path, table.schema)

            writer.write_table(table)

    if writer:
        writer.close()

    print(f"Done. Games processed: {total:,}")
    print(f"Saved to: {output_path}")

write_parquet_stream(INPUT, OUTPUT)