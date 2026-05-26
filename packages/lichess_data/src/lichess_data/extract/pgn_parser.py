from __future__ import annotations

import chess.pgn


class PGNParser:
    """Parses chess.pgn game objects into flat dictionaries."""
 
    @staticmethod
    def _safe_int(x) -> int | None:
        if x is None or x in ("?", ""):
            return None
        try:
            return int(x)
        except (ValueError, TypeError):
            return None
 
    def parse(self, game: chess.pgn.Game) -> dict:
        """Parse a single PGN game into a flat record dict."""
        headers = game.headers
        moves = []
        node = game
        while not node.is_end():
            node = node.variation(0)
            if node.move:
                moves.append(node.move.uci())
 
        return {
            "event":              headers.get("Event"),
            "site":               headers.get("Site"),
            "date":               headers.get("Date"),
            "round":              headers.get("Round"),
            "white":              headers.get("White"),
            "black":              headers.get("Black"),
            "result":             headers.get("Result"),
            "utc_date":           headers.get("UTCDate"),
            "utc_time":           headers.get("UTCTime"),
            "white_elo":          self._safe_int(headers.get("WhiteElo")),
            "black_elo":          self._safe_int(headers.get("BlackElo")),
            "white_rating_diff":  self._safe_int(headers.get("WhiteRatingDiff")),
            "black_rating_diff":  self._safe_int(headers.get("BlackRatingDiff")),
            "eco":                headers.get("ECO"),
            "opening":            headers.get("Opening"),
            "time_control":       headers.get("TimeControl"),
            "termination":        headers.get("Termination"),
            "moves":              moves,
        }
