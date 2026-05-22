$out_dir = 'build';
$pdf_mode = 1;        # pdflatex (diagram PDFs are Ghostscript-normalised for embedding)
$bibtex_use = 2;      # biber
$interaction = 'nonstopmode';

# Ensure main.tex is the default target when running latexmk in this dir
@default_files = ('main.tex');

# Regenerate main-body word count before each LaTeX run (see scripts/gen-body-wordcount.sh).
$wordcount_script = './scripts/gen-body-wordcount.sh';
if (-x $wordcount_script) {
    system($wordcount_script) == 0 or warn "wordcount: $wordcount_script failed\n";
} else {
    warn "wordcount: $wordcount_script missing or not executable\n";
}
