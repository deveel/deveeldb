
EXTRA_DIST =  Makefile

# Warning: This is an automatically generated file, do not edit!

srcdir=.
top_srcdir=..

include $(top_srcdir)/config.make

ifeq ($(CONFIG),DEBUG)
ASSEMBLY_COMPILER_COMMAND = gmcs
ASSEMBLY_COMPILER_FLAGS =  -noconfig -codepage:utf8 -warn:3 -optimize- -debug "-define:TRACE;DEBUG;NET_2_0"
ASSEMBLY = bin/Debug/libdeveeldb.dll
ASSEMBLY_MDB = $(ASSEMBLY).mdb
COMPILE_TARGET = library
PROJECT_REFERENCES = 
BUILD_DIR = bin/Debug/

LIBDEVEELDB_DLL_MDB_SOURCE=bin/Debug/libdeveeldb.dll.mdb

endif

ifeq ($(CONFIG),RELEASE)
ASSEMBLY_COMPILER_COMMAND = gmcs
ASSEMBLY_COMPILER_FLAGS =  -noconfig -codepage:utf8 -warn:4 -optimize+ -debug -define:DEBUG "-define:TRACE"
ASSEMBLY = bin/Release/libdeveeldb.dll
ASSEMBLY_MDB = $(ASSEMBLY).mdb
COMPILE_TARGET = library
PROJECT_REFERENCES = 
BUILD_DIR = bin/Release/

LIBDEVEELDB_DLL_MDB_SOURCE=bin/Release/libdeveeldb.dll.mdb

endif

ifeq ($(CONFIG),DEBUG___MONO)
ASSEMBLY_COMPILER_COMMAND = gmcs
ASSEMBLY_COMPILER_FLAGS =  -noconfig -codepage:utf8 -warn:3 -optimize- -debug "-define:TRACE;DEBUG;NET_2_0;MONO"
ASSEMBLY = bin/Debug/Mono/libdeveeldb.dll
ASSEMBLY_MDB = $(ASSEMBLY).mdb
COMPILE_TARGET = library
PROJECT_REFERENCES = 
BUILD_DIR = bin/Debug/Mono/

LIBDEVEELDB_DLL_MDB_SOURCE=bin/Debug/Mono/libdeveeldb.dll.mdb

endif

AL=al2
SATELLITE_ASSEMBLY_NAME=$(notdir $(basename $(ASSEMBLY))).resources.dll

PROGRAMFILES = \
	$(LIBDEVEELDB_DLL_MDB)  

LINUX_PKGCONFIG = \
	$(DEVEELDB_PC)  


RESGEN=resgen2

DEVEELDB_PC = $(BUILD_DIR)/deveeldb.pc
LIBDEVEELDB_DLL_MDB = $(BUILD_DIR)/libdeveeldb.dll.mdb

FILES = \
	AssemblyInfo.cs \
	Deveel.Data.Client/ConnectionString.cs \
	Deveel.Data.Client/DeveelDbCommand.cs \
	Deveel.Data.Client/DeveelDbConnection.cs \
	Deveel.Data.Client/DeveelDbConnectionStringBuilder.cs \
	Deveel.Data.Client/DeveelDbDataReader.cs \
	Deveel.Data.Client/DeveelDbParameter.cs \
	Deveel.Data.Client/DeveelDbParameterCollection.cs \
	Deveel.Data.Client/DeveelDbTransaction.cs \
	Deveel.Data.Client/DeveelDbTrigger.cs \
	Deveel.Data.Client/ParemeterStyle.cs \
	Deveel.Data.Client/TriggerEventMoment.cs \
	Deveel.Data.Client/TriggerEventTypes.cs \
	Deveel.Data.Control/ConfigProperty.cs \
	Deveel.Data.Sql/CommitStatement.cs \
	Deveel.Data.Sql/ConstraintType.cs \
	Deveel.Data.Sql/CreateFunctionStatement.cs \
	Deveel.Data.Sql/CreateSchemaStatement.cs \
	Deveel.Data.Sql/DropFunctionStatement.cs \
	Deveel.Data.Sql/DropSchemaStatement.cs \
	Deveel.Data.Sql/RollbackStatement.cs \
	Deveel.Data.Sql/SqlParseException.cs \
	Deveel.Data.Store/FSync.cs \
	Deveel.Data.Store/IFSync.cs \
	Deveel.Data.Store/SyncFailedException.cs \
	Deveel.Data.Text/DeveelRegexLibrary.cs \
	Deveel.Data.Util/Properties.cs \
	Deveel.Data.Util/ShellUserTerminal.cs \
	Deveel.Data/DatabaseEventHandler.cs \
	Deveel.Data/ReferenceType.cs \
	Deveel.Diagnostics/Debug.cs \
	Deveel.Diagnostics/DefaultDebugLogger.cs \
	Deveel.Diagnostics/IDebugLogger.cs \
	Deveel.Diagnostics/DebugLevel.cs \
	Deveel.Data.Functions/AbsFunction.cs \
	Deveel.Data.Functions/AggOrFunction.cs \
	Deveel.Data.Functions/AvgFunction.cs \
	Deveel.Data.Functions/BinaryToHexFunction.cs \
	Deveel.Data.Functions/CoalesceFunction.cs \
	Deveel.Data.Functions/ConcatFunction.cs \
	Deveel.Data.Functions/CurrValFunction.cs \
	Deveel.Data.Functions/DateFormatFunction.cs \
	Deveel.Data.Functions/DateObFunction.cs \
	Deveel.Data.Functions/DistinctCountFunction.cs \
	Deveel.Data.Functions/ForeignRuleConvert.cs \
	Deveel.Data.Functions/GreatestFunction.cs \
	Deveel.Data.Functions/HexToBinaryFunction.cs \
	Deveel.Data.Functions/IfFunction.cs \
	Deveel.Data.Functions/ObjectInstantiation.cs \
	Deveel.Data.Functions/ObjectInstantiation2.cs \
	Deveel.Data.Functions/LeastFunction.cs \
	Deveel.Data.Functions/LengthFunction.cs \
	Deveel.Data.Functions/LowerFunction.cs \
	Deveel.Data.Functions/LTrimFunction.cs \
	Deveel.Data.Functions/MaxFunction.cs \
	Deveel.Data.Functions/MinFunction.cs \
	Deveel.Data.Functions/ModFunction.cs \
	Deveel.Data.Functions/NextValFunction.cs \
	Deveel.Data.Functions/PowFunction.cs \
	Deveel.Data.Functions/PrivGroupsFunction.cs \
	Deveel.Data.Functions/PrivilegeString.cs \
	Deveel.Data.Functions/RoundFunction.cs \
	Deveel.Data.Functions/RTrimFunction.cs \
	Deveel.Data.Functions/SetValFunction.cs \
	Deveel.Data.Functions/SignFunction.cs \
	Deveel.Data.Functions/SQLCastFunction.cs \
	Deveel.Data.Functions/SQLTrimFunction.cs \
	Deveel.Data.Functions/SQLTypeString.cs \
	Deveel.Data.Functions/SqrtFunction.cs \
	Deveel.Data.Functions/SubstringFunction.cs \
	Deveel.Data.Functions/SumFunction.cs \
	Deveel.Data.Functions/TimeObFunction.cs \
	Deveel.Data.Functions/TimeStampObFunction.cs \
	Deveel.Data.Functions/ToNumberFunction.cs \
	Deveel.Data.Functions/UniqueKeyFunction.cs \
	Deveel.Data.Functions/UpperFunction.cs \
	Deveel.Data.Functions/UserFunction.cs \
	Deveel.Data.Functions/ViewDataConvert.cs \
	Deveel.Data.Sql/UserStatement.cs \
	Deveel.Data.Text/CollationBoundMode.cs \
	Deveel.Data.Text/CollationDecomposition.cs \
	Deveel.Data.Text/CollationKey.cs \
	Deveel.Data.Text/CollationStrength.cs \
	Deveel.Data.Text/DoubleMetaphone.cs \
	Deveel.Data.Text/ICollator.cs \
	Deveel.Data.Text/ICollatorFactory.cs \
	Deveel.Data.Text/IRegexLibrary.cs \
	Deveel.Data.Text/ISearchEngine.cs \
	Deveel.Data.Text/Metaphone.cs \
	Deveel.Data.Text/RefinedSoundex.cs \
	Deveel.Data.Text/SearchResult.cs \
	Deveel.Data.Text/SearchTextRow.cs \
	Deveel.Data.Text/Soundex.cs \
	Deveel.Data.Text/SystemCollatorFactory.cs \
	Deveel.Data.Text/SystemRegexLibrary.cs \
	Deveel.Data/AccessType.cs \
	Deveel.Data.Functions/CountFunction.cs \
	Deveel.Data/CompositeFunction.cs \
	Deveel.Data/GrantObject.cs \
	Deveel.Data/JoinType.cs \
	Deveel.Data/JournalCommand.cs \
	Deveel.Data/LockingMode.cs \
	Deveel.Data/OperatorSet.cs \
	Deveel.Data/ProductInfo.cs \
	Deveel.Data/TriggerEventType.cs \
	Deveel.Math/BigDecimal.cs \
	Deveel.Math/BigInteger.cs \
	Deveel.Math/DecimalRoundingMode.cs \
	Deveel.Math/MPN.cs \
	Deveel.Math/Number.cs \
	Deveel.Math/NumberState.cs \
	Deveel.Data.Server/DatabaseInterfaceBase.cs \
	Deveel.Data.Server/DefaultLocalBootable.cs \
	Deveel.Data.Server/DatabaseInterface.cs \
	Deveel.Data.Client/StreamableObject.cs \
	Deveel.Data.Client/IBlob.cs \
	Deveel.Data.Client/IClob.cs \
	Deveel.Data.Client/IDatabaseCallBack.cs \
	Deveel.Data.Client/IDatabaseInterface.cs \
	Deveel.Data.Client/ILocalBootable.cs \
	Deveel.Data.Client/DbBlob.cs \
	Deveel.Data.Client/DbClob.cs \
	Deveel.Data.Client/ResultSet.cs \
	Deveel.Data.Client/DbDataException.cs \
	Deveel.Data.Client/DbStreamableBlob.cs \
	Deveel.Data.Client/DbStreamableClob.cs \
	Deveel.Data.Client/ProtocolConstants.cs \
	Deveel.Data.Client/IQueryResponse.cs \
	Deveel.Data.Client/RemoteDatabaseInterface.cs \
	Deveel.Data.Client/ResultPart.cs \
	Deveel.Data.Client/RowCache.cs \
	Deveel.Data.Client/SqlCommand.cs \
	Deveel.Data.Client/StreamableObjectPart.cs \
	Deveel.Data.Client/StreamDatabaseInterface.cs \
	Deveel.Data.Client/TCPStreamDatabaseInterface.cs \
	Deveel.Data.Client/ITriggerListener.cs \
	Deveel.Data.Control/DbConfig.cs \
	Deveel.Data.Control/IDbConfig.cs \
	Deveel.Data.Control/DbController.cs \
	Deveel.Data.Control/DbSystem.cs \
	Deveel.Data.Control/DefaultDbConfig.cs \
	Deveel.Data/IBlobAccessor.cs \
	Deveel.Data/IBlobRef.cs \
	Deveel.Data/ByteLongObject.cs \
	Deveel.Data/CastHelper.cs \
	Deveel.Data/IClobRef.cs \
	Deveel.Data/ColumnDescription.cs \
	Deveel.Data/ObjectTransfer.cs \
	Deveel.Data/ObjectTranslator.cs \
	Deveel.Data/IRef.cs \
	Deveel.Data/SQLTypes.cs \
	Deveel.Data/StreamableObject.cs \
	Deveel.Data/IStringAccessor.cs \
	Deveel.Data/StringObject.cs \
	Deveel.Data/DbTypes.cs \
	Deveel.Data/TypeUtil.cs \
	Deveel.Data.Sql/AlterTableStatement.cs \
	Deveel.Data.Sql/AlterTableAction.cs \
	Deveel.Data.Sql/ByColumn.cs \
	Deveel.Data.Sql/CallStatement.cs \
	Deveel.Data.Sql/ColumnChecker.cs \
	Deveel.Data.Sql/ColumnDef.cs \
	Deveel.Data.Sql/CompactStatement.cs \
	Deveel.Data.Sql/CompleteTransactionStatement.cs \
	Deveel.Data.Sql/ConstraintDef.cs \
	Deveel.Data.Sql/CreateTableStatement.cs \
	Deveel.Data.Sql/CreateTriggerStatement.cs \
	Deveel.Data.Sql/DeleteStatement.cs \
	Deveel.Data.Sql/DropTableStatement.cs \
	Deveel.Data.Sql/DropTriggerStatement.cs \
	Deveel.Data.Sql/FromClause.cs \
	Deveel.Data.Sql/FromTableDef.cs \
	Deveel.Data.Sql/FromTableDirectSource.cs \
	Deveel.Data.Sql/IFromTable.cs \
	Deveel.Data.Sql/FromTableSubQuerySource.cs \
	Deveel.Data.Sql/FunctionStatement.cs \
	Deveel.Data.Sql/InsertStatement.cs \
	Deveel.Data.Sql/ShutdownStatement.cs \
	Deveel.Data.Sql/NoOpStatement.cs \
	Deveel.Data.Sql/Planner.cs \
	Deveel.Data.Sql/PrivilegesStatement.cs \
	Deveel.Data.Sql/SchemaStatement.cs \
	Deveel.Data.Sql/SearchExpression.cs \
	Deveel.Data.Sql/SelectStatement.cs \
	Deveel.Data.Sql/SelectColumn.cs \
	Deveel.Data.Sql/SequenceStatement.cs \
	Deveel.Data.Sql/SetStatement.cs \
	Deveel.Data.Sql/ShowStatement.cs \
	Deveel.Data.Sql/SqlCommandExecutor.cs \
	Deveel.Data.Sql/Statement.cs \
	Deveel.Data.Sql/TableExpressionFromSet.cs \
	Deveel.Data.Sql/TableSelectExpression.cs \
	Deveel.Data.Sql/UpdateTableStatement.cs \
	Deveel.Data.Sql/ViewStatement.cs \
	Deveel.Data.Functions/AggregateFunction.cs \
	Deveel.Data/DataTableBase.cs \
	Deveel.Data.Functions/Function.cs \
	Deveel.Data/InternalTableInfo.cs \
	Deveel.Data/InternalTableInfo2.cs \
	Deveel.Data/QueryContext.cs \
	Deveel.Data/Assignment.cs \
	Deveel.Data/BlindSearch.cs \
	Deveel.Data/BlobStore.cs \
	Deveel.Data.Store/IBlobStore.cs \
	Deveel.Data/Caster.cs \
	Deveel.Data/CollatedBaseSearch.cs \
	Deveel.Data/CompositeTable.cs \
	Deveel.Data/ConnectionTriggerManager.cs \
	Deveel.Data/CorrelatedVariable.cs \
	Deveel.Data/Database.cs \
	Deveel.Data/DatabaseConnection.cs \
	Deveel.Data/DatabaseConstraintViolationException.cs \
	Deveel.Data/DatabaseDispatcher.cs \
	Deveel.Data/DatabaseException.cs \
	Deveel.Data/IDatabaseProcedure.cs \
	Deveel.Data/DatabaseQueryContext.cs \
	Deveel.Data/DatabaseSystem.cs \
	Deveel.Data/DataCellCache.cs \
	Deveel.Data/DataIndexDef.cs \
	Deveel.Data/DataIndexSetDef.cs \
	Deveel.Data/DataTable.cs \
	Deveel.Data/DataTableColumnDef.cs \
	Deveel.Data/DataTableDef.cs \
	Deveel.Data/IDataTableListener.cs \
	Deveel.Data/DefaultDataTable.cs \
	Deveel.Data/DumpHelper.cs \
	Deveel.Data/Expression.cs \
	Deveel.Data/IExpressionPreparer.cs \
	Deveel.Data/FilterTable.cs \
	Deveel.Data.Store/FixedRecordList.cs \
	Deveel.Data.Functions/IFunction.cs \
	Deveel.Data.Functions/FunctionDef.cs \
	Deveel.Data.Functions/FunctionFactory.cs \
	Deveel.Data.Functions/IFunctionInfo.cs \
	Deveel.Data.Functions/IFunctionLookup.cs \
	Deveel.Data/FunctionTable.cs \
	Deveel.Data.Functions/FunctionType.cs \
	Deveel.Data/GrantManager.cs \
	Deveel.Data/IGroupResolver.cs \
	Deveel.Data/GTConnectionInfoDataSource.cs \
	Deveel.Data/GTCurrentConnectionsDataSource.cs \
	Deveel.Data/GTDataSource.cs \
	Deveel.Data/GTPrivMapDataSource.cs \
	Deveel.Data/GTProductDataSource.cs \
	Deveel.Data/GTSQLTypeInfoDataSource.cs \
	Deveel.Data/GTStatisticsDataSource.cs \
	Deveel.Data/GTTableColumnsDataSource.cs \
	Deveel.Data/GTTableInfoDataSource.cs \
	Deveel.Data/ImportedKey.cs \
	Deveel.Data/IIndexSet.cs \
	Deveel.Data/IndexSetStore.cs \
	Deveel.Data/INHelper.cs \
	Deveel.Data/InsertSearch.cs \
	Deveel.Data.Functions/InternalFunctionFactory.cs \
	Deveel.Data/InternalDbHelper.cs \
	Deveel.Data/IInternalTableInfo.cs \
	Deveel.Data/JoinedTable.cs \
	Deveel.Data/JoiningSet.cs \
	Deveel.Data/Lock.cs \
	Deveel.Data/LockHandle.cs \
	Deveel.Data/LockingMechanism.cs \
	Deveel.Data/LockingQueue.cs \
	Deveel.Data/MasterTableDataSource.cs \
	Deveel.Data/MasterTableGC.cs \
	Deveel.Data/MasterTableJournal.cs \
	Deveel.Data/MultiVersionTableIndices.cs \
	Deveel.Data/IMutableTableDataSource.cs \
	Deveel.Data/NaturallyJoinedTable.cs \
	Deveel.Data/OpenTransactionList.cs \
	Deveel.Data/Operator.cs \
	Deveel.Data/OuterTable.cs \
	Deveel.Data/ParameterSubstitution.cs \
	Deveel.Data/PatternSearch.cs \
	Deveel.Data/Privileges.cs \
	Deveel.Data/IProcedureConnection.cs \
	Deveel.Data/ProcedureException.cs \
	Deveel.Data/ProcedureManager.cs \
	Deveel.Data/ProcedureName.cs \
	Deveel.Data/IQueryContext.cs \
	Deveel.Data/QueryPlan.cs \
	Deveel.Data/IQueryPlanNode.cs \
	Deveel.Data/IRawDiagnosticTable.cs \
	Deveel.Data/RawTableInformation.cs \
	Deveel.Data/RecordState.cs \
	Deveel.Data/ReferenceTable.cs \
	Deveel.Data/RIDList.cs \
	Deveel.Data/IRootTable.cs \
	Deveel.Data/RowData.cs \
	Deveel.Data/IRowEnumerator.cs \
	Deveel.Data/SchemaDef.cs \
	Deveel.Data/SelectableRange.cs \
	Deveel.Data/SelectableRangeSet.cs \
	Deveel.Data/SelectableScheme.cs \
	Deveel.Data/SequenceManager.cs \
	Deveel.Data/SimpleRowEnumerator.cs \
	Deveel.Data/SimpleTableQuery.cs \
	Deveel.Data/SimpleTransaction.cs \
	Deveel.Data.Sql/StatementCache.cs \
	Deveel.Data/StatementException.cs \
	Deveel.Data.Sql/StatementTree.cs \
	Deveel.Data/IStatementTreeObject.cs \
	Deveel.Data/StateStore.cs \
	Deveel.Data/IStoreSystem.cs \
	Deveel.Data/SubsetColumnTable.cs \
	Deveel.Data/SystemQueryContext.cs \
	Deveel.Data/Table.cs \
	Deveel.Data/TableAccessState.cs \
	Deveel.Data/TableBackedCache.cs \
	Deveel.Data/TableCommitModificationEvent.cs \
	Deveel.Data/TableDataConglomerate.cs \
	Deveel.Data/ITableDataSource.cs \
	Deveel.Data/TableFunctions.cs \
	Deveel.Data/TableModificationEvent.cs \
	Deveel.Data/TableName.cs \
	Deveel.Data/ITableQueryDef.cs \
	Deveel.Data/TArrayType.cs \
	Deveel.Data/TBinaryType.cs \
	Deveel.Data/TBooleanType.cs \
	Deveel.Data/TDateType.cs \
	Deveel.Data/TemporaryTable.cs \
	Deveel.Data/TObjectType.cs \
	Deveel.Data/TNullType.cs \
	Deveel.Data/TNumericType.cs \
	Deveel.Data/TObject.cs \
	Deveel.Data/TQueryPlanType.cs \
	Deveel.Data/Transaction.cs \
	Deveel.Data/TransactionException.cs \
	Deveel.Data/TransactionJournal.cs \
	Deveel.Data/TransactionModificationListener.cs \
	Deveel.Data/TransactionSystem.cs \
	Deveel.Data/TriggerEvent.cs \
	Deveel.Data/ITriggerListener.cs \
	Deveel.Data/TriggerManager.cs \
	Deveel.Data/TStringType.cs \
	Deveel.Data/TType.cs \
	Deveel.Data/User.cs \
	Deveel.Data/UserAccessException.cs \
	Deveel.Data/UserManager.cs \
	Deveel.Data/V1FileStoreSystem.cs \
	Deveel.Data/V1HeapStoreSystem.cs \
	Deveel.Data/V2MasterTableDataSource.cs \
	Deveel.Data/Variable.cs \
	Deveel.Data/IVariableResolver.cs \
	Deveel.Data/ViewDef.cs \
	Deveel.Data/ViewManager.cs \
	Deveel.Data/VirtualTable.cs \
	Deveel.Data/WorkerPool.cs \
	Deveel.Data/WorkerThread.cs \
	Deveel.Data.Store/AbstractStore.cs \
	Deveel.Data.Store/IArea.cs \
	Deveel.Data.Store/IAreaWriter.cs \
	Deveel.Data.Store/HeapStore.cs \
	Deveel.Data.Store/IOStoreDataAccessor.cs \
	Deveel.Data.Store/JournalledFileStore.cs \
	Deveel.Data.Store/IJournalledResource.cs \
	Deveel.Data.Store/JournalledSystem.cs \
	Deveel.Data.Store/LoggingBufferManager.cs \
	Deveel.Data.Store/IMutableArea.cs \
	Deveel.Data.Store/ScatteringStoreDataAccessor.cs \
	Deveel.Data.Store/IStore.cs \
	Deveel.Data.Store/IStoreDataAccessor.cs \
	Deveel.Data.Store/StreamFile.cs \
	Deveel.Data.Collections/AbstractBlockIntegerList.cs \
	Deveel.Math/BigNumber.cs \
	Deveel.Data.Collections/BlockIntegerList.cs \
	Deveel.Data.Util/ByteBuffer.cs \
	Deveel.Data.Util/Cache.cs \
	Deveel.Data.Util/HashMapList.cs \
	Deveel.Data.Collections/IIndexComparer.cs \
	Deveel.Data.Collections/IIntegerIterator.cs \
	Deveel.Data.Collections/IntegerListBlockInterface.cs \
	Deveel.Data.Collections/IIntegerList.cs \
	Deveel.Data.Collections/IntegerVector.cs \
	Deveel.Diagnostics/Log.cs \
	Deveel.Diagnostics/LogWriter.cs \
	Deveel.Data.Util/PagedInputStream.cs \
	Deveel.Data.Util/Stats.cs \
	Deveel.Data.Util/IUserTerminal.cs \
	Deveel.Data.Procedure/SystemBackup.cs \
	Deveel.Data.Sql/ParseException.cs \
	Deveel.Data.Sql/SimpleCharStream.cs \
	Deveel.Data.Sql/SQL.cs \
	Deveel.Data.Sql/SQLConstants.cs \
	Deveel.Data.Sql/SQLTokenManager.cs \
	Deveel.Data.Sql/Token.cs \
	Deveel.Data.Sql/TokenMgrError.cs \
	Deveel.Data.Sql/Util.cs \
	Deveel.Data/IDatabaseEvent.cs \
	Deveel.Text.RegularExpressions/BufferedStreamManager.cs \
	Deveel.Text.RegularExpressions/BufferedTextReader.cs \
	Deveel.Text.RegularExpressions/CharIndexedCharArray.cs \
	Deveel.Text.RegularExpressions/CharIndexedReader.cs \
	Deveel.Text.RegularExpressions/CharIndexedStream.cs \
	Deveel.Text.RegularExpressions/CharIndexedString.cs \
	Deveel.Text.RegularExpressions/CharIndexedStringBuilder.cs \
	Deveel.Text.RegularExpressions/ICharIndexed.cs \
	Deveel.Text.RegularExpressions/Regex.cs \
	Deveel.Text.RegularExpressions/RegexErrorType.cs \
	Deveel.Text.RegularExpressions/RegexException.cs \
	Deveel.Text.RegularExpressions/RegexFilterStream.cs \
	Deveel.Text.RegularExpressions/RegexFilterTextReader.cs \
	Deveel.Text.RegularExpressions/RegexFlags.cs \
	Deveel.Text.RegularExpressions/RegexMatch.cs \
	Deveel.Text.RegularExpressions/RegexMatchEnumerator.cs \
	Deveel.Text.RegularExpressions/RegexSyntax.cs \
	Deveel.Text.RegularExpressions/RegexToken.cs \
	Deveel.Text.RegularExpressions/RegexTokenAny.cs \
	Deveel.Text.RegularExpressions/RegexTokenBackRef.cs \
	Deveel.Text.RegularExpressions/RegexTokenChar.cs \
	Deveel.Text.RegularExpressions/RegexTokenEnd.cs \
	Deveel.Text.RegularExpressions/RegexTokenEndSub.cs \
	Deveel.Text.RegularExpressions/RegexTokenLookAhead.cs \
	Deveel.Text.RegularExpressions/RegexTokenOneOf.cs \
	Deveel.Text.RegularExpressions/RegexTokenPOSIX.cs \
	Deveel.Text.RegularExpressions/RegexTokenRange.cs \
	Deveel.Text.RegularExpressions/RegexTokenRepeated.cs \
	Deveel.Text.RegularExpressions/RegexTokenStart.cs \
	Deveel.Text.RegularExpressions/RegexTokenWordBoundary.cs \
	Deveel.Text.RegularExpressions/SyntaxKey.cs \
	Deveel.Text.RegularExpressions/UncheckedRegex.cs \
	Deveel.Zip/Adler32.cs \
	Deveel.Zip/CheckedStream.cs \
	Deveel.Zip/Checksum.cs \
	Deveel.Zip/CompressionLevel.cs \
	Deveel.Zip/CompressionStrategy.cs \
	Deveel.Zip/CRC32.cs \
	Deveel.Zip/Deflater.cs \
	Deveel.Zip/DeflaterConstants.cs \
	Deveel.Zip/DeflaterEngine.cs \
	Deveel.Zip/DeflaterHuffman.cs \
	Deveel.Zip/DeflaterOutputStream.cs \
	Deveel.Zip/DeflaterPending.cs \
	Deveel.Zip/GZipInputStream.cs \
	Deveel.Zip/Inflater.cs \
	Deveel.Zip/InflaterDynHeader.cs \
	Deveel.Zip/InflaterHuffmanTree.cs \
	Deveel.Zip/InflaterInputStream.cs \
	Deveel.Zip/OutputWindow.cs \
	Deveel.Zip/PendingBuffer.cs \
	Deveel.Zip/ShiftUtil.cs \
	Deveel.Zip/StreamManipulator.cs \
	Deveel.Zip/ZipException.cs 

DATA_FILES = 

RESOURCES = 

EXTRAS = \
	deveeldb.pc.in 

REFERENCES =  \
	System \
	System.Data \
	System.Transactions \
	System.Xml

DLL_REFERENCES = 

CLEANFILES = $(PROGRAMFILES) $(LINUX_PKGCONFIG) 

#Targets
all: $(ASSEMBLY) $(PROGRAMFILES) $(LINUX_PKGCONFIG)  $(top_srcdir)/config.make

include $(top_srcdir)/Makefile.include
#include $(srcdir)/custom-hooks.make



$(eval $(call emit-deploy-wrapper,DEVEELDB_PC,deveeldb.pc))


$(eval $(call emit_resgen_targets))
$(build_xamlg_list): %.xaml.g.cs: %.xaml
	xamlg '$<'

# Targets for Custom commands
DEBUG_BeforeBuild:
	(cd $(srcdir) && java -classpath ..\util\csharpcc.jar csharpcc -OUTPUT_DIRECTORY=Deveel.Data.Sql Deveel.Data.Sql\SQL.csc)

DEBUG - MONO_BeforeBuild:
	(cd $(srcdir) && java -classpath $(srcdir)/../util/csharpcc.jar csharpcc -OUTPUT_DIRECTORY=$(srcdir)/Deveel.Data.Sql $(srcdir)/Deveel.Data.Sql/SQL.csc)


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
	mkdir -p '$(DESTDIR)$(libdir)/pkgconfig'
	$(call cp,$(DEVEELDB_PC),$(DESTDIR)$(libdir)/pkgconfig)
	$(call cp,$(LIBDEVEELDB_DLL_MDB),$(DESTDIR)$(libdir)/$(PACKAGE))
	make post-install-local-hook prefix=$(prefix)

uninstall-local: $(ASSEMBLY) $(ASSEMBLY_MDB)
	make pre-uninstall-local-hook prefix=$(prefix)
	make uninstall-satellite-assemblies prefix=$(prefix)
	$(call rm,$(ASSEMBLY),$(DESTDIR)$(libdir)/$(PACKAGE))
	$(call rm,$(ASSEMBLY_MDB),$(DESTDIR)$(libdir)/$(PACKAGE))
	$(call rm,$(DEVEELDB_PC),$(DESTDIR)$(libdir)/pkgconfig)
	$(call rm,$(LIBDEVEELDB_DLL_MDB),$(DESTDIR)$(libdir)/$(PACKAGE))
	make post-uninstall-local-hook prefix=$(prefix)

install: install-local
uninstall: uninstall-local
clean: clean-local

include $(top_srcdir)/rules.make
