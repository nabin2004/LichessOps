# CMP6230 report — build PDF into build/ (see .latexmkrc)
MAIN   := main.tex
LATEXMK := latexmk

.PHONY: pdf watch clean distclean

pdf:
	$(LATEXMK) -pdf -interaction=nonstopmode $(MAIN)

watch:
	$(LATEXMK) -pdf -pvc -interaction=nonstopmode $(MAIN)

clean:
	rm -rf build
	$(LATEXMK) -c $(MAIN) 2>/dev/null || true

distclean: clean
