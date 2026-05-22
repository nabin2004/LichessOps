# Vendor logos (optional PNG drop zone)

## Institution cover logo (title page)

The themed cover in [`content/cover-page.tex`](../../content/cover-page.tex) looks for a Birmingham City University mark here first:

| Expected filename | Use | Print size hint |
|-------------------|-----|----------------|
| `bcu-logo.png` **or** `bcu-logo.pdf` | Strip above the banner text on page~1 | **18--22\,mm** height (the template requests 18\,mm; keep margins clear in the 32\,mm oxford-blue band). |

`\graphicspath` is set in [`preamble.tex`](../../preamble.tex); the cover uses `\IfFileExists` on `figures/logos/bcu-logo.png` then `figures/logos/bcu-logo.pdf` (bare `bcu-logo.png` would fail for `\IfFileExists` because it does not use `\graphicspath`). Until you drop either file here, the cover builds typography-only without error.

Supply official BCU branding from your institutional press/media kit where required.

---

The TikZ macros in [`tikz/mlops-logos.tex`](../../tikz/mlops-logos.tex) look for files in this directory **before** falling back to the [Simple Icons](https://simpleicons.org/) font glyphs (monochrome).

Place **PNG** or **PDF** files named exactly as below (no extra suffix beyond `.png` / `.pdf`). `\graphicspath` already includes `figures/logos/`.

| Expected filename | Tool | Official brand / press assets (download manually) |
|-------------------|------|-----------------------------------------------------|
| `airflow.png` | Apache Airflow | [Apache Airflow logos](https://apache.org/logos/) |
| `kafka.png` | Apache Kafka | [Apache Kafka logos](https://apache.org/logos/) / [Confluent brand](https://www.confluent.io/press/) (use Apache marks where appropriate) |
| `spark.png` | Apache Spark | [Apache Spark logos](https://spark.apache.org/images/) |
| `mlflow.png` | MLflow | [LF AI & Data MLflow trademark](https://lfaidata.foundation/projects/mlflow/) |
| `docker.png` | Docker | [Docker brand guidelines](https://www.docker.com/company/newsroom/media-resources/) |
| `kubernetes.png` | Kubernetes | [Kubernetes brand](https://github.com/kubernetes/kubernetes/tree/master/logo) (CNCF) |
| `prometheus.png` | Prometheus | [Prometheus logo](https://prometheus.io/) (CNCF / project site) |
| `grafana.png` | Grafana Labs | [Grafana press kit](https://grafana.com/about/press/) |
| `dbt.png` | dbt Labs | [dbt brand](https://www.getdbt.com/) (press / brand assets) |
| `jupyter.png` | Project Jupyter | [Jupyter trademark](https://jupyter.org/) |

**Tip:** Keep icons **simple and legible at print size** (roughly 7–10\,mm height in the diagram). Very wide wordmarks may need cropping or a square icon variant.

Until these files exist, the PDF builds using Simple Icons glyphs automatically.
