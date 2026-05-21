$out_dir = 'build';
$pdf_mode = 1;        # pdflatex (diagram PDFs are Ghostscript-normalised for embedding)
$bibtex_use = 2;      # biber
$interaction = 'nonstopmode';

# Ensure main.tex is the default target when running latexmk in this dir
@default_files = ('main.tex');
