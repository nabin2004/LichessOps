# Figures for `report/build/main.pdf`

Most pipeline diagrams are raster/vector exports checked in beside this file (`elt.pdf`, `dataIngestion.pdf`, …). Ghostscript normalization for pdfLaTeX is documented in the project [`README.md`](../README.md).

## Chapter 3 placeholders (Exercise 1, Step 3)

When you draft storage-specific artwork, export **PDF** (ideally PDF 1.5 after normalization) using these filenames so [`content/03-storage-plan.tex`](../content/03-storage-plan.tex) picks them up automatically:

| Filename | Diagram intent |
|----------|----------------|
| `lake-partitions.pdf` | Twin MinIO buckets: raw `.pgn.zst` prefixes vs curated Parquet prefixes with `year=/month=` partitioning. |
| `star-schema-erd.pdf` | Crow’s-foot ERD: `fact_games`, `dim_player`, `dim_opening` / `eco`, `dim_date`, optional `fact_moves`. |
| `storage-elt.pdf` | Optional single-page DAG consolidating Lichess $\rightarrow$ Airflow $\rightarrow$ raw MinIO $\rightarrow$ Spark $\rightarrow$ partitioned Parquet + validation gate (the skeleton already cross-references `elt-flow` / `data-ingestion`). |

Until those files exist, the chapter renders **boxed placeholders** (`\IfFileExists{…}` branches in the source).
