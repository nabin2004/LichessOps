# Report 2 — standalone LaTeX build
MAIN = report2
LATEXMK = latexmk
LATEXMK_FLAGS = -pdf -interaction=nonstopmode -file-line-error

.PHONY: all pdf wordcount clean distclean

all: pdf

pdf: wordcount
	$(LATEXMK) $(LATEXMK_FLAGS) $(MAIN).tex

wordcount:
	@./scripts/gen-body-wordcount.sh

clean:
	$(LATEXMK) -c $(MAIN).tex

distclean:
	$(LATEXMK) -C $(MAIN).tex
