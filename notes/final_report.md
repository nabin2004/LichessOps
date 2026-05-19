
I am treating this project as if this MLOPS pipeline is being deployed on lichess and treating myself as an real engineer at Lichess :-)

# #1 
I am going to treat the 2013 Jan's data as our recent data and train it on the first week of the data then slowly move to next week and so on and so forth.

# #2
We need `Stockfish Engine Manager` for imputing this 
1. [%eval]
2. [%clk] --> For this we need to lookup on what to choose.


## zstandard: 
Zstandard (zstd) is a high-performance, lossless data compression algorithm developed by Meta. It balances speed and compression ratios, providing faster decompression and better overall compression compared to older tools like gzip or zlib.

1. Initially used the zstandard python library but it seems that this package is not official. switched to `from compression import zstd`s

# tools used:
- for unzipping lichess data: `zstandard`

# Missing Values Issues

## 1. Anonymous players: 
The platforms allows the players to play anonymously, I think most probably that's the reason behind the missing Elos for black and white.

white_elo             78
black_elo            140
white_rating_diff    218
black_rating_diff    218

