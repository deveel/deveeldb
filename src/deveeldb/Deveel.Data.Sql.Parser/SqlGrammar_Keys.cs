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
		private KeyTerm AFTER;
		private KeyTerm ASC;
		private KeyTerm BEFORE;
		private KeyTerm BEGIN;
		private KeyTerm BETWEEN;
		private KeyTerm BINARY;
		private KeyTerm BLOB;
		private KeyTerm BOOLEAN;
		private KeyTerm BY;
		private KeyTerm CALL;
		private KeyTerm CALLBACK;
		private KeyTerm CASCADE;
		private KeyTerm CASE;
		private KeyTerm CAST;
		private KeyTerm CHECK;
		private KeyTerm CONSTRAINT;
		private KeyTerm CREATE;
		private KeyTerm DECLARE;
		private KeyTerm DEFAULT;
		private KeyTerm DELETE;
		private KeyTerm EACH;
		private KeyTerm END;
		private KeyTerm ELSE;
		private KeyTerm ELSIF;
		private KeyTerm EXECUTE;
		private KeyTerm EXISTS;
		private KeyTerm FOR;
		private KeyTerm FOREIGN;
		private KeyTerm GROUPS;
		private KeyTerm KEY;
		private KeyTerm IDENTITY;
		private KeyTerm IF;
		private KeyTerm INDEX;
		private KeyTerm INSERT;
		private KeyTerm LOCK;
		private KeyTerm NOT;
		private KeyTerm NULL;
		private KeyTerm ON;
		private KeyTerm OR;
		private KeyTerm PRIMARY;
		private KeyTerm PROCEDURE;
		private KeyTerm REFERENCES;
		private KeyTerm REPLACE;
		private KeyTerm ROW;
		private KeyTerm SEQUENCE;
		private KeyTerm SET;
		private KeyTerm TABLE;
		private KeyTerm THEN;
		private KeyTerm TRIGGER;
		private KeyTerm TYPE;
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
				ASC.Text,
				BEFORE.Text,
				BEGIN.Text,
				BETWEEN.Text,
				BINARY.Text,
				BLOB.Text,
				BOOLEAN.Text,
				BY.Text,
				CALL.Text,
				CASCADE.Text,
				CASE.Text,
				CAST.Text,
				INSERT.Text,
				SEQUENCE.Text,
				TRIGGER.Text
			};

			MarkReservedWords(reserved.ToArray());
		}

		protected override void Keywords() {
			ACCOUNT = ToTerm("ACCOUNT");
			AFTER = ToTerm("AFTER");
			ASC = ToTerm("ASC");
			BEFORE = ToTerm("BEFORE");
			BEGIN = ToTerm("BEGIN");
			BETWEEN = ToTerm("BETWEEN");
			BINARY = ToTerm("BINARY");
			BLOB = ToTerm("BLOB");
			BOOLEAN = ToTerm("BOOLEAN");
			BY = ToTerm("BY");
			CALL = ToTerm("CALL");
			CALLBACK = ToTerm("CALLBACK");
			CASCADE = ToTerm("CASCADE");
			CASE = ToTerm("CASE");
			CAST = ToTerm("CAST");
			CHECK = ToTerm("CHECK");
			CONSTRAINT = ToTerm("CONSTRAINT");
			CREATE = ToTerm("CREATE");
			DECLARE = ToTerm("DECLARE");
			DELETE = ToTerm("DELETE");
			DEFAULT = ToTerm("DEFAULT");
			EACH = ToTerm("EACH");
			END = ToTerm("END");
			ELSE = ToTerm("ELSE");
			ELSIF = ToTerm("ELSIF");
			EXECUTE = ToTerm("EXECUTE");
			EXISTS = ToTerm("EXISTS");
			FOR = ToTerm("FOR");
			FOREIGN = ToTerm("FOREIGN");
			GROUPS = ToTerm("GROUPS");
			KEY = ToTerm("KEY");
			IDENTITY = ToTerm("IDENTITY");
			IF = ToTerm("IF");
			INDEX = ToTerm("INDEX");
			INSERT = ToTerm("INSERT");
			LOCK = ToTerm("LOCK");
			NOT = ToTerm("NOT");
			NULL = ToTerm("NULL");
			ON = ToTerm("ON");
			OR = ToTerm("OR");
			PRIMARY = ToTerm("PRIMARY");
			PROCEDURE = ToTerm("PROCEDURE");
			REPLACE = ToTerm("REPLACE");
			REFERENCES = ToTerm("REFERENCES");
			ROW = ToTerm("ROW");
			SEQUENCE = ToTerm("SEQUENCE");
			SET = ToTerm("SET");
			TABLE = ToTerm("TABLE");
			THEN = ToTerm("THEN");
			TRIGGER = ToTerm("TRIGGER");
			TYPE = ToTerm("TYPE");
			UPDATE = ToTerm("UPDATE");
			UNIQUE = ToTerm("UNIQUE");
			UNLOCK = ToTerm("UNLOCK");
			VIEW = ToTerm("VIEW");
		}
	}
}
