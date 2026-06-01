# CMP6230 report — build PDF into build/ (see .latexmkrc)
MAIN      := main.tex
FORMATIVE := main-formative.tex
LATEXMK   := latexmk

.PHONY: pdf pdf-formative watch clean distclean

pdf:
	./scripts/gen-body-wordcount.sh $(MAIN)
	$(LATEXMK) -pdf -interaction=nonstopmode $(MAIN)

pdf-formative:
	./scripts/build-pdf.sh $(FORMATIVE)

watch:
	$(LATEXMK) -pdf -pvc -interaction=nonstopmode $(MAIN)

clean:
	rm -rf build
	$(LATEXMK) -c $(MAIN) 2>/dev/null || true
	rm -f main.aux main.bbl main.bcf main.blg main.fdb_latexmk main.fls main.lof main.log main.lot main.out main.pdf main.run.xml main.synctex.gz main.toc

distclean: clean
