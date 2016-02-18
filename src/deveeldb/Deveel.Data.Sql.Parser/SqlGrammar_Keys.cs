// 
//  Copyright 2010-2015 Deveel
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
using System.Collections.Generic;

using Irony.Parsing;

namespace Deveel.Data.Sql.Parser {
	partial class SqlGrammar {
		#region Keywords

		private KeyTerm ACCOUNT;
		private KeyTerm CHECK;
		private KeyTerm CONSTRAINT;
		private KeyTerm CREATE;
		private KeyTerm DELETE;
		private KeyTerm EXISTS;
		private KeyTerm FOREIGN;
		private KeyTerm GROUPS;
		private KeyTerm KEY;
		private KeyTerm IF;
		private KeyTerm INDEX;
		private KeyTerm LOCK;
		private KeyTerm NOT;
		private KeyTerm ON;
		private KeyTerm OR;
		private KeyTerm PRIMARY;
		private KeyTerm REFERENCES;
		private KeyTerm SET;
		private KeyTerm TABLE;
		private KeyTerm UPDATE;
		private KeyTerm UNIQUE;
		private KeyTerm UNLOCK;
		private KeyTerm VIEW;

		#endregion

		protected override void ReservedWords() {
			var reserved = new List<string> {
				"ABSOLUTE",
				"ACTION",
				"ADD",
				"AFTER",
				"ALL",
				"ALTER",
				"AND",
				"ANY",
				"ARRAY",
				"AS",
				"ASC",
				"BEFORE",
				"BEGIN",
				"BETWEEN",
				"BINARY",
				"BLOB",
				"BOOLEAN",
				"BY",
				"CALL",
				"CASCADE",
				"CASE",
				"CAST",
				"INSERT",
				"SEQUENCE",
				"TRIGGER"
			};

			MarkReservedWords(reserved.ToArray());
		}

		protected override void Keywords() {
			ACCOUNT = ToTerm("ACCOUNT");
			CHECK = ToTerm("CHECK");
			CONSTRAINT = ToTerm("CONSTRAINT");
			CREATE = ToTerm("CREATE");
			DELETE = ToTerm("DELETE");
			EXISTS = ToTerm("EXISTS");
			FOREIGN = ToTerm("FOREIGN");
			GROUPS = ToTerm("GROUPS");
			KEY = ToTerm("KEY");
			IF = ToTerm("IF");
			INDEX = ToTerm("INDEX");
			LOCK = ToTerm("LOCK");
			NOT = ToTerm("NOT");
			ON = ToTerm("ON");
			OR = ToTerm("OR");
			PRIMARY = ToTerm("PRIMARY");
			REFERENCES = ToTerm("REFERENCES");
			SET = ToTerm("SET");
			TABLE = ToTerm("TABLE");
			UPDATE = ToTerm("UPDATE");
			UNIQUE = ToTerm("UNIQUE");
			UNLOCK = ToTerm("UNLOCK");
			VIEW = ToTerm("VIEW");
		}
	}
}
