VERSION=0.2.0
COMPONENT=snarl

build_info:
	pkg_info -X pkg_install | egrep '^(MACHINE_ARCH|OPSYS|OS_VERSION|PKGTOOLS_VERSION)' >build-info

package: build_info
	pkg_create -B build-info -c comment -d description -f packlist -I /opt/local/$(COMPONENT) -p files -U $(COMPONENT)-$(VERSION).tgz

clean:
	-rm -r files/*
	-rm build-info
