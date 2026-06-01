$out_dir = 'build';
$pdf_mode = 1;        # pdflatex (diagram PDFs are Ghostscript-normalised for embedding)
$bibtex_use = 2;      # biber
$interaction = 'nonstopmode';

# Ensure main.tex is the default target when running latexmk in this dir
@default_files = ('main.tex');
for my $arg (@ARGV) {
    if ($arg =~ /\.tex$/i && $arg !~ /^-/) {
        @default_files = ($arg);
        last;
    }
}

# Word count is run from the Makefile before latexmk (see scripts/gen-body-wordcount.sh).
# Do not run it here at rc load time: that updates build/bodywordcount.tex after latexmk
# starts and can trigger overlapping pdflatex runs that corrupt .lof/.aux files.
