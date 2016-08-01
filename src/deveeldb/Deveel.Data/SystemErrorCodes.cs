// 
//  Copyright 2010-2016 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//


using System;

namespace Deveel.Data {
	public static class SystemErrorCodes {
		// System errors: 100XXX
		public const int UnknownSystemError = 100001;
		public const int ObjectNotFound = 100030;
		public const int StatementExecutionError = 100041;

		// SQL Model Errors: 101XXX
		public const int PrimaryKeyViolation = 101011;
		public const int UniqueKeyViolation = 101012;
		public const int CheckViolation = 101013;
		public const int ForeignKeyViolation = 101014;
		public const int NotNullColumnViolation = 101015;
		public const int TableDropViolation = 101016;
		public const int ColumnDropViolation = 101017;
		public const int TypeDropViolation = 101018;

		public const int NotNullVariableViolation = 101020;
		public const int ConstantVariableViolation = 101021;

		public const int UnknownExpressionError = 101040;
		public const int InvalidExpressionFormat = 101041;
		public const int ExpressionEvaluation = 101042;

		public const int MissingPrimaryKey = 101052;

		public const int CursorGeneralError = 101060;
		public const int CursorOutOfContext = 101061;
		public const int CursorFetchError = 101063;
		public const int CursorOutOfBounds = 101064;
		public const int CursorOpen = 101065;
		public const int CursorClosed = 101066;
		public const int ScrollCursorFetch = 101067;

		// Transaction Error: 102XXX
		public const int ReadOnlyTransaction = 102002;
		public const int DirtySelectInTransaction = 102023;
		public const int DroppedModifiedObjectConflict = 102031;
		public const int DuplicateObjectConflict = 102032;
		public const int NonCommittedConflict = 102033;
		public const int RowRemoveConflict = 102034;

		// I/O Errors: 501XXX
		public const int UnknownStorageError = 501101;
		public const int StorageReadError = 501105;

		public const int ObjectLengthViolation = 501201;
		public const int InvalidObjectId = 501202;
	}
}