APPNAME=erlsvn
ERLC_FLAGS=
SOURCES=$(wildcard src/*.erl)
HEADERS=$(wildcard src/*.hrl) $(wildcard include/*.hrl)
OBJECTS=$(SOURCES:src/%.erl=ebin/%.beam)
DOC_OPTS=
all: $(OBJECTS) test
ebin/%.beam : src/%.erl $(HEADERS) Makefile
	erlc $(ERLC_FLAGS) -o ebin/ $<
docs:
	erl -pa ./ebin -noshell -eval "edoc:application($(APPNAME), \".\", [$(DOC_OPTS)])" -s init stop
clean:
	-rm -f $(OBJECTS)
	-rm -f doc/edoc-info doc/*.html doc/*.css doc/*.png
test:
	erl -noshell -pa ebin \
	 -eval 'eunit:test("ebin",[])' \
	 -s init stop

release: clean
	$(MAKE) ERLC_FLAGS="$(ERLC_FLAGS) -DNOTEST"
