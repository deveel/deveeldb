
EXTRA_DIST =  Makefile

# Warning: This is an automatically generated file, do not edit!

srcdir=.
top_srcdir=..\deveeldb

include $(top_srcdir)/config.make

ifeq ($(CONFIG),DEBUG)
ASSEMBLY_COMPILER_COMMAND = gmcs
ASSEMBLY_COMPILER_FLAGS =  -noconfig -codepage:utf8 -warn:4 -optimize- -debug "-define:DEBUG;TRACE"
ASSEMBLY = bin/Debug/deveeldb-nunit.dll
ASSEMBLY_MDB = $(ASSEMBLY).mdb
COMPILE_TARGET = library
PROJECT_REFERENCES =  \
	../deveeldb/bin/Debug/deveeldb.dll
BUILD_DIR = bin/Debug/

DEVEELDB_DLL_SOURCE=../deveeldb/bin/Debug/deveeldb.dll
ICSHARPCODE_SHARPZIPLIB_DLL_SOURCE=../libs/ICSharpCode.SharpZipLib.dll
DEVEELDB_NUNIT_DLL_MDB_SOURCE=bin/Debug/deveeldb-nunit.dll.mdb
DEVEELDB_NUNIT_DLL_MDB=$(BUILD_DIR)/deveeldb-nunit.dll.mdb

endif

ifeq ($(CONFIG),RELEASE)
ASSEMBLY_COMPILER_COMMAND = gmcs
ASSEMBLY_COMPILER_FLAGS =  -noconfig -codepage:utf8 -warn:4 -optimize+ "-define:TRACE"
ASSEMBLY = bin/Release/deveeldb-nunit.dll
ASSEMBLY_MDB = 
COMPILE_TARGET = library
PROJECT_REFERENCES =  \
	../deveeldb/bin/Release/deveeldb.dll
BUILD_DIR = bin/Release/

DEVEELDB_DLL_SOURCE=../deveeldb/bin/Release/deveeldb.dll
ICSHARPCODE_SHARPZIPLIB_DLL_SOURCE=../libs/ICSharpCode.SharpZipLib.dll
DEVEELDB_NUNIT_DLL_MDB=

endif

AL=al2
SATELLITE_ASSEMBLY_NAME=$(notdir $(basename $(ASSEMBLY))).resources.dll

PROGRAMFILES = \
	$(DEVEELDB_DLL) \
	$(ICSHARPCODE_SHARPZIPLIB_DLL) \
	$(DEVEELDB_NUNIT_DLL_MDB)  

LINUX_PKGCONFIG = \
	$(DEVEELDB_NUNIT_PC)  


RESGEN=resgen2

DEVEELDB_DLL = $(BUILD_DIR)/deveeldb.dll
ICSHARPCODE_SHARPZIPLIB_DLL = $(BUILD_DIR)/ICSharpCode.SharpZipLib.dll
DEVEELDB_NUNIT_PC = $(BUILD_DIR)/deveeldb-nunit.pc

FILES = \
	Deveel.Data/CreateDatabaseTest.cs \
	Deveel.Data/TestBase.cs \
	Properties/AssemblyInfo.cs 

DATA_FILES = 

RESOURCES = 

EXTRAS = \
	deveeldb-nunit.pc.in 

REFERENCES =  \
	nunit.framework \
	System \
	System.Data \
	System.Xml

DLL_REFERENCES = 

CLEANFILES = $(PROGRAMFILES) $(LINUX_PKGCONFIG) 

#Targets
all: $(ASSEMBLY) $(PROGRAMFILES) $(LINUX_PKGCONFIG)  $(top_srcdir)/config.make

include $(top_srcdir)/Makefile.include
#include $(srcdir)/custom-hooks.make



$(eval $(call emit-deploy-target,DEVEELDB_DLL))
$(eval $(call emit-deploy-target,ICSHARPCODE_SHARPZIPLIB_DLL))
$(eval $(call emit-deploy-wrapper,DEVEELDB_NUNIT_PC,deveeldb-nunit.pc))


$(eval $(call emit_resgen_targets))
$(build_xamlg_list): %.xaml.g.cs: %.xaml
	xamlg '$<'


$(ASSEMBLY_MDB): $(ASSEMBLY)
$(ASSEMBLY): $(build_sources) $(build_resources) $(build_datafiles) $(DLL_REFERENCES) $(PROJECT_REFERENCES) $(build_xamlg_list) $(build_satellite_assembly_list)
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
	mkdir -p '$(DESTDIR)$(libdir)\$(PACKAGE)'
	$(call cp,$(DEVEELDB_DLL),$(DESTDIR)$(libdir)\$(PACKAGE))
	$(call cp,$(ICSHARPCODE_SHARPZIPLIB_DLL),$(DESTDIR)$(libdir)\$(PACKAGE))
	$(call cp,$(DEVEELDB_NUNIT_DLL_MDB),$(DESTDIR)$(libdir)\$(PACKAGE))
	mkdir -p '$(DESTDIR)$(libdir)\pkgconfig'
	$(call cp,$(DEVEELDB_NUNIT_PC),$(DESTDIR)$(libdir)\pkgconfig)
	make post-install-local-hook prefix=$(prefix)

uninstall-local: $(ASSEMBLY) $(ASSEMBLY_MDB)
	make pre-uninstall-local-hook prefix=$(prefix)
	make uninstall-satellite-assemblies prefix=$(prefix)
	$(call rm,$(ASSEMBLY),$(DESTDIR)$(libdir)/$(PACKAGE))
	$(call rm,$(ASSEMBLY_MDB),$(DESTDIR)$(libdir)/$(PACKAGE))
	$(call rm,$(DEVEELDB_DLL),$(DESTDIR)$(libdir)\$(PACKAGE))
	$(call rm,$(ICSHARPCODE_SHARPZIPLIB_DLL),$(DESTDIR)$(libdir)\$(PACKAGE))
	$(call rm,$(DEVEELDB_NUNIT_DLL_MDB),$(DESTDIR)$(libdir)\$(PACKAGE))
	$(call rm,$(DEVEELDB_NUNIT_PC),$(DESTDIR)$(libdir)\pkgconfig)
	make post-uninstall-local-hook prefix=$(prefix)

install: install-local
uninstall: uninstall-local
clean: clean-local

include $(top_srcdir)/rules.make
