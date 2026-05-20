$out_dir = 'build';
$pdf_mode = 5;        # pdflatex
$bibtex_use = 2;      # biber
$interaction = 'nonstopmode';

# Ensure main.tex is the default target when running latexmk in this dir
@default_files = ('main.tex');
