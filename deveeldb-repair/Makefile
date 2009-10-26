
EXTRA_DIST =  Makefile

# Warning: This is an automatically generated file, do not edit!

srcdir=.
top_srcdir=..

include $(top_srcdir)/config.make

ifeq ($(CONFIG),DEBUG)
ASSEMBLY_COMPILER_COMMAND = gmcs
ASSEMBLY_COMPILER_FLAGS =  -noconfig -codepage:utf8 -warn:4 -optimize- -debug "-define:DEBUG;TRACE"
ASSEMBLY = bin/Debug/deveeldb-repair.exe
ASSEMBLY_MDB = $(ASSEMBLY).mdb
COMPILE_TARGET = exe
PROJECT_REFERENCES =  \
	../deveeldb/bin/Debug/libdeveeldb.dll
BUILD_DIR = bin/Debug/

LIBDEVEELDB_DLL_SOURCE=../deveeldb/bin/Debug/libdeveeldb.dll
ICSHARPCODE_SHARPZIPLIB_DLL_SOURCE=../libs/ICSharpCode.SharpZipLib.dll
DEVEELDB_REPAIR_EXE_MDB_SOURCE=bin/Debug/deveeldb-repair.exe.mdb
DEVEELDB_REPAIR_EXE_MDB=$(BUILD_DIR)/deveeldb-repair.exe.mdb

endif

ifeq ($(CONFIG),RELEASE)
ASSEMBLY_COMPILER_COMMAND = gmcs
ASSEMBLY_COMPILER_FLAGS =  -noconfig -codepage:utf8 -warn:4 -optimize+ "-define:TRACE"
ASSEMBLY = bin/Release/deveeldb-repair.exe
ASSEMBLY_MDB = 
COMPILE_TARGET = exe
PROJECT_REFERENCES =  \
	../deveeldb/bin/Release/libdeveeldb.dll
BUILD_DIR = bin/Release/

LIBDEVEELDB_DLL_SOURCE=../deveeldb/bin/Release/libdeveeldb.dll
ICSHARPCODE_SHARPZIPLIB_DLL_SOURCE=../libs/ICSharpCode.SharpZipLib.dll
DEVEELDB_REPAIR_EXE_MDB=

endif

AL=al2
SATELLITE_ASSEMBLY_NAME=$(notdir $(basename $(ASSEMBLY))).resources.dll

PROGRAMFILES = \
	$(LIBDEVEELDB_DLL) \
	$(ICSHARPCODE_SHARPZIPLIB_DLL) \
	$(DEVEELDB_REPAIR_EXE_MDB)  

BINARIES = \
	$(DEVEELDB_REPAIR)  


RESGEN=resgen2

LIBDEVEELDB_DLL = $(BUILD_DIR)/libdeveeldb.dll
ICSHARPCODE_SHARPZIPLIB_DLL = $(BUILD_DIR)/ICSharpCode.SharpZipLib.dll
DEVEELDB_REPAIR = $(BUILD_DIR)/deveeldb-repair

FILES = \
	Deveel.Data/Program.cs \
	Properties/AssemblyInfo.cs 

DATA_FILES = 

RESOURCES = 

EXTRAS = \
	Deveel.Data.Util \
	deveeldb-repair.in 

REFERENCES =  \
	System \
	System.Data \
	System.Xml

DLL_REFERENCES = 

CLEANFILES = $(PROGRAMFILES) $(BINARIES) 

#Targets
all: $(ASSEMBLY) $(PROGRAMFILES) $(BINARIES)  $(top_srcdir)/config.make

include $(top_srcdir)/Makefile.include
#include $(srcdir)/custom-hooks.make



$(eval $(call emit-deploy-target,LIBDEVEELDB_DLL))
$(eval $(call emit-deploy-target,ICSHARPCODE_SHARPZIPLIB_DLL))
$(eval $(call emit-deploy-wrapper,DEVEELDB_REPAIR,deveeldb-repair,x))


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
	$(call cp,$(LIBDEVEELDB_DLL),$(DESTDIR)$(libdir)\$(PACKAGE))
	$(call cp,$(ICSHARPCODE_SHARPZIPLIB_DLL),$(DESTDIR)$(libdir)\$(PACKAGE))
	$(call cp,$(DEVEELDB_REPAIR_EXE_MDB),$(DESTDIR)$(libdir)\$(PACKAGE))
	mkdir -p '$(DESTDIR)$(bindir)'
	$(call cp,$(DEVEELDB_REPAIR),$(DESTDIR)$(bindir))
	make post-install-local-hook prefix=$(prefix)

uninstall-local: $(ASSEMBLY) $(ASSEMBLY_MDB)
	make pre-uninstall-local-hook prefix=$(prefix)
	make uninstall-satellite-assemblies prefix=$(prefix)
	$(call rm,$(ASSEMBLY),$(DESTDIR)$(libdir)/$(PACKAGE))
	$(call rm,$(ASSEMBLY_MDB),$(DESTDIR)$(libdir)/$(PACKAGE))
	$(call rm,$(LIBDEVEELDB_DLL),$(DESTDIR)$(libdir)\$(PACKAGE))
	$(call rm,$(ICSHARPCODE_SHARPZIPLIB_DLL),$(DESTDIR)$(libdir)\$(PACKAGE))
	$(call rm,$(DEVEELDB_REPAIR_EXE_MDB),$(DESTDIR)$(libdir)\$(PACKAGE))
	$(call rm,$(DEVEELDB_REPAIR),$(DESTDIR)$(bindir))
	make post-uninstall-local-hook prefix=$(prefix)

install: install-local
uninstall: uninstall-local
clean: clean-local

include $(top_srcdir)/rules.make
