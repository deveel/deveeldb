
EXTRA_DIST =  Makefile

# Warning: This is an automatically generated file, do not edit!

srcdir=.
top_srcdir=..

include $(top_srcdir)/config.make

ifeq ($(CONFIG),DEBUG)
ASSEMBLY_COMPILER_COMMAND = gmcs
ASSEMBLY_COMPILER_FLAGS =  -noconfig -codepage:utf8 -warn:3 -optimize- -debug "-define:DEBUG"
ASSEMBLY = bin/Debug/deveeldb-nunit.dll
ASSEMBLY_MDB = $(ASSEMBLY).mdb
COMPILE_TARGET = library
PROJECT_REFERENCES =  \
	../deveeldb/bin/Debug/libdeveeldb.dll
BUILD_DIR = bin/Debug/

DEVEELDB_NUNIT_DLL_MDB_SOURCE=bin/Debug/deveeldb-nunit.dll.mdb
NUNIT_FRAMEWORK_DLL_SOURCE=../lib/nunit.framework.dll
LIBDEVEELDB_DLL_SOURCE=../deveeldb/bin/Debug/libdeveeldb.dll
NUNIT_CORE_DLL_SOURCE=../lib/nunit.core.dll
LIBDEVEELDB_DLL_MDB_SOURCE=../deveeldb/bin/Debug/libdeveeldb.dll.mdb
LIBDEVEELDB_DLL_MDB=$(BUILD_DIR)/libdeveeldb.dll.mdb

endif

ifeq ($(CONFIG),RELEASE)
ASSEMBLY_COMPILER_COMMAND = gmcs
ASSEMBLY_COMPILER_FLAGS =  -noconfig -codepage:utf8 -warn:4 -optimize- -debug -define:DEBUG
ASSEMBLY = bin/Release/deveeldb-nunit.dll
ASSEMBLY_MDB = $(ASSEMBLY).mdb
COMPILE_TARGET = library
PROJECT_REFERENCES =  \
	../deveeldb/bin/Release/libdeveeldb.dll
BUILD_DIR = bin/Release/

DEVEELDB_NUNIT_DLL_MDB_SOURCE=bin/Release/deveeldb-nunit.dll.mdb
NUNIT_FRAMEWORK_DLL_SOURCE=../lib/nunit.framework.dll
LIBDEVEELDB_DLL_SOURCE=../deveeldb/bin/Release/libdeveeldb.dll
NUNIT_CORE_DLL_SOURCE=../lib/nunit.core.dll
LIBDEVEELDB_DLL_MDB=

endif

ifeq ($(CONFIG),DEBUG___MONO)
ASSEMBLY_COMPILER_COMMAND = gmcs
ASSEMBLY_COMPILER_FLAGS =  -noconfig -codepage:utf8 -warn:3 -optimize- -debug "-define:DEBUG;TRACE;MONO"
ASSEMBLY = bin/Debug/Mono/deveeldb-nunit.dll
ASSEMBLY_MDB = $(ASSEMBLY).mdb
COMPILE_TARGET = library
PROJECT_REFERENCES =  \
	../deveeldb/bin/Debug/Mono/libdeveeldb.dll
BUILD_DIR = bin/Debug/Mono

DEVEELDB_NUNIT_DLL_MDB_SOURCE=bin/Debug/Mono/deveeldb-nunit.dll.mdb
NUNIT_FRAMEWORK_DLL_SOURCE=../lib/nunit.framework.dll
LIBDEVEELDB_DLL_SOURCE=../deveeldb/bin/Debug/Mono/libdeveeldb.dll
NUNIT_CORE_DLL_SOURCE=../lib/nunit.core.dll
LIBDEVEELDB_DLL_MDB_SOURCE=../deveeldb/bin/Debug/Mono/libdeveeldb.dll.mdb
LIBDEVEELDB_DLL_MDB=$(BUILD_DIR)/libdeveeldb.dll.mdb

endif

AL=al2
SATELLITE_ASSEMBLY_NAME=$(notdir $(basename $(ASSEMBLY))).resources.dll

PROGRAMFILES = \
	$(DEVEELDB_NUNIT_DLL_MDB) \
	$(NUNIT_FRAMEWORK_DLL) \
	$(LIBDEVEELDB_DLL) \
	$(NUNIT_CORE_DLL) \
	$(LIBDEVEELDB_DLL_MDB)  

LINUX_PKGCONFIG = \
	$(DEVEELDB_NUNIT_PC)  


RESGEN=resgen2

DEVEELDB_NUNIT_DLL_MDB = $(BUILD_DIR)/deveeldb-nunit.dll.mdb
NUNIT_FRAMEWORK_DLL = $(BUILD_DIR)/nunit.framework.dll
LIBDEVEELDB_DLL = $(BUILD_DIR)/libdeveeldb.dll
NUNIT_CORE_DLL = $(BUILD_DIR)/nunit.core.dll
DEVEELDB_NUNIT_PC = $(BUILD_DIR)/deveeldb-nunit.pc

FILES = \
	Deveel.Data/CreateDatabaseTest.cs \
	Deveel.Data/TestBase.cs \
	AssemblyInfo.cs \
	Deveel.Data/QueryTest.cs 

DATA_FILES = 

RESOURCES = 

EXTRAS = \
	deveeldb-nunit.pc.in 

REFERENCES =  \
	System \
	System.Data

DLL_REFERENCES =  \
	../lib/nunit.core.dll \
	../lib/nunit.framework.dll

CLEANFILES = $(PROGRAMFILES) $(LINUX_PKGCONFIG) 

#Targets
all: $(ASSEMBLY) $(PROGRAMFILES) $(LINUX_PKGCONFIG)  $(top_srcdir)/config.make

include $(top_srcdir)/Makefile.include
#include $(srcdir)/custom-hooks.make



$(eval $(call emit-deploy-target,NUNIT_FRAMEWORK_DLL))
$(eval $(call emit-deploy-target,LIBDEVEELDB_DLL))
$(eval $(call emit-deploy-target,NUNIT_CORE_DLL))
$(eval $(call emit-deploy-wrapper,DEVEELDB_NUNIT_PC,deveeldb-nunit.pc))
$(eval $(call emit-deploy-target,LIBDEVEELDB_DLL_MDB))


$(eval $(call emit_resgen_targets))
$(build_xamlg_list): %.xaml.g.cs: %.xaml
	xamlg '$<'


$(ASSEMBLY) $(ASSEMBLY_MDB): $(build_sources) $(build_resources) $(build_datafiles) $(DLL_REFERENCES) $(PROJECT_REFERENCES) $(build_xamlg_list) $(build_satellite_assembly_list)
	make pre-all-local-hook prefix=$(prefix)
	mkdir -p $(shell dirname $(ASSEMBLY))
	make $(CONFIG)_BeforeBuild
	$(ASSEMBLY_COMPILER_COMMAND) $(ASSEMBLY_COMPILER_FLAGS) -out:$(ASSEMBLY) -target:$(COMPILE_TARGET) $(build_sources_embed) $(build_resources_embed) $(build_references_ref)
	make $(CONFIG)_AfterBuild
	make post-all-local-hook prefix=$(prefix)

install-local: $(ASSEMBLY) $(ASSEMBLY_MDB)
	make pre-install-local-hook prefix=$(prefix)
	make install-satellite-assemblies prefix=$(prefix)
	mkdir -p '$(DESTDIR)$(libdir)/$(PACKAGE)'
	$(call cp,$(ASSEMBLY),$(DESTDIR)$(libdir)/$(PACKAGE))
	$(call cp,$(ASSEMBLY_MDB),$(DESTDIR)$(libdir)/$(PACKAGE))
	$(call cp,$(DEVEELDB_NUNIT_DLL_MDB),$(DESTDIR)$(libdir)/$(PACKAGE))
	$(call cp,$(NUNIT_FRAMEWORK_DLL),$(DESTDIR)$(libdir)/$(PACKAGE))
	$(call cp,$(LIBDEVEELDB_DLL),$(DESTDIR)$(libdir)/$(PACKAGE))
	$(call cp,$(NUNIT_CORE_DLL),$(DESTDIR)$(libdir)/$(PACKAGE))
	mkdir -p '$(DESTDIR)$(libdir)/pkgconfig'
	$(call cp,$(DEVEELDB_NUNIT_PC),$(DESTDIR)$(libdir)/pkgconfig)
	$(call cp,$(LIBDEVEELDB_DLL_MDB),$(DESTDIR)$(libdir)/$(PACKAGE))
	make post-install-local-hook prefix=$(prefix)

uninstall-local: $(ASSEMBLY) $(ASSEMBLY_MDB)
	make pre-uninstall-local-hook prefix=$(prefix)
	make uninstall-satellite-assemblies prefix=$(prefix)
	$(call rm,$(ASSEMBLY),$(DESTDIR)$(libdir)/$(PACKAGE))
	$(call rm,$(ASSEMBLY_MDB),$(DESTDIR)$(libdir)/$(PACKAGE))
	$(call rm,$(DEVEELDB_NUNIT_DLL_MDB),$(DESTDIR)$(libdir)/$(PACKAGE))
	$(call rm,$(NUNIT_FRAMEWORK_DLL),$(DESTDIR)$(libdir)/$(PACKAGE))
	$(call rm,$(LIBDEVEELDB_DLL),$(DESTDIR)$(libdir)/$(PACKAGE))
	$(call rm,$(NUNIT_CORE_DLL),$(DESTDIR)$(libdir)/$(PACKAGE))
	$(call rm,$(DEVEELDB_NUNIT_PC),$(DESTDIR)$(libdir)/pkgconfig)
	$(call rm,$(LIBDEVEELDB_DLL_MDB),$(DESTDIR)$(libdir)/$(PACKAGE))
	make post-uninstall-local-hook prefix=$(prefix)

install: install-local
uninstall: uninstall-local
clean: clean-local

include $(top_srcdir)/rules.make
