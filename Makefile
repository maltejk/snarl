REBAR = $(shell pwd)/rebar3

.PHONY: rel stagedevrel version all

all: cp-hooks compile

cp-hooks:
	cp hooks/* .git/hooks

version:
	@echo "$(shell git symbolic-ref HEAD 2> /dev/null | cut -b 12-)-$(shell git log --pretty=format:'%h, %ad' -1)" > snarl.version

version_header: version
	@echo "-define(VERSION, <<\"$(shell cat snarl.version)\">>)." > apps/snarl/include/snarl_version.hrl

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean
	make -C rel/pkg clean

long-test:
	$(REBAR) eunit -DEQC_LONG_TESTS -v
eunit: 
	$(REBAR) compile
	$(REBAR) eunit -v

test: eunit
	$(REBAR) xref

quick-xref:
	$(REBAR) xref

quick-test:
	$(REBAR) as eqc eunit -v

update:
	$(REBAR) update
	
rel: update
	$(REBAR) as prod compile
	sh generate_zabbix_template.sh
	$(REBAR) release

package: rel
	make -C rel/pkg package

###
### Docs
###
docs:
	$(REBAR) skip_deps=true doc

##
## Developer targets
##

xref: all
	$(REBAR) xref

##
## Dialyzer
##
APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	xmerl webtool snmp public_key mnesia eunit syntax_tools compiler

dialyzer: deps compile
	$(REBAR) dialyzer -Wno_return | grep -v -f dialyzer.mittigate

typer:
	typer --plt ./_build/default/rebar3_*_plt _build/default/lib/*/ebin
