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

		private KeyTerm ABSOLUTE;
		private KeyTerm ACCOUNT;
		private KeyTerm ALL;
		private KeyTerm ACTION;
		private KeyTerm ADD;
		private KeyTerm AFTER;
		private KeyTerm ALTER;
		private KeyTerm AND;
		private KeyTerm ANY;
		private KeyTerm ARRAY;
		private KeyTerm AS;
		private KeyTerm ASC;
		private KeyTerm BEFORE;
		private KeyTerm BEGIN;
		private KeyTerm BETWEEN;
		private KeyTerm BINARY;
		private KeyTerm BLOB;
		private KeyTerm BOOLEAN;
		private KeyTerm BY;
		private KeyTerm CACHE;
		private KeyTerm CALL;
		private KeyTerm CALLBACK;
		private KeyTerm CASCADE;
		private KeyTerm CASE;
		private KeyTerm CAST;
		private KeyTerm CHECK;
		private KeyTerm CONSTANT;
		private KeyTerm CONSTRAINT;
		private KeyTerm CREATE;
		private KeyTerm CURSOR;
		private KeyTerm CYCLE;
		private KeyTerm DECLARE;
		private KeyTerm DEFAULT;
		private KeyTerm DELETE;
		private KeyTerm EACH;
		private KeyTerm END;
		private KeyTerm ELSE;
		private KeyTerm ELSIF;
		private KeyTerm EXCEPTION;
		private KeyTerm EXCEPTION_INIT;
		private KeyTerm EXECUTE;
		private KeyTerm EXISTS;
		private KeyTerm EXIT;
		private KeyTerm EXTERNALLY;
		private KeyTerm FOR;
		private KeyTerm FOREIGN;
		private KeyTerm GOTO;
		private KeyTerm GROUPS;
		private KeyTerm KEY;
		private KeyTerm IDENTIFIED;
		private KeyTerm IDENTITY;
		private KeyTerm IF;
		private KeyTerm INCREMENT;
		private KeyTerm INDEX;
		private KeyTerm INSERT;
		private KeyTerm IS;
		private KeyTerm LOCK;
		private KeyTerm MAXVALUE;
		private KeyTerm MINVALUE;
		private KeyTerm NO;
		private KeyTerm NOT;
		private KeyTerm NULL;
		private KeyTerm ON;
		private KeyTerm OR;
		private KeyTerm PASSWORD;
		private KeyTerm PRIMARY;
		private KeyTerm PRAGMA;
		private KeyTerm PROCEDURE;
		private KeyTerm REFERENCES;
		private KeyTerm REPLACE;
		private KeyTerm ROW;
		private KeyTerm SCHEMA;
		private KeyTerm SEQUENCE;
		private KeyTerm SET;
		private KeyTerm START;
		private KeyTerm TABLE;
		private KeyTerm THEN;
		private KeyTerm TRIGGER;
		private KeyTerm TYPE;
		private KeyTerm UPDATE;
		private KeyTerm UNIQUE;
		private KeyTerm UNLOCK;
		private KeyTerm USER;
		private KeyTerm VIEW;
		private KeyTerm WHEN;
		private KeyTerm WITH;

		#endregion

		protected override void ReservedWords() {
			var reserved = new List<string> {
				ABSOLUTE.Text,
				ACTION.Text,
				ADD.Text,
				AFTER.Text,
				ALL.Text,
				ALTER.Text,
				AND.Text,
				ANY.Text,
				ARRAY.Text,
				AS.Text,
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
			ABSOLUTE = ToTerm("ABSOLUTE");
			ACTION = ToTerm("ACTION");
			ACCOUNT = ToTerm("ACCOUNT");
			ALL = ToTerm("ALL");
			ALTER = ToTerm("ALTER");
			ADD = ToTerm("ADD");
			AFTER = ToTerm("AFTER");
			AND = ToTerm("AND");
			ANY = ToTerm("ANY");
			ARRAY = ToTerm("ARRAY");
			AS = ToTerm("AS");
			ASC = ToTerm("ASC");
			BEFORE = ToTerm("BEFORE");
			BEGIN = ToTerm("BEGIN");
			BETWEEN = ToTerm("BETWEEN");
			BINARY = ToTerm("BINARY");
			BLOB = ToTerm("BLOB");
			BOOLEAN = ToTerm("BOOLEAN");
			BY = ToTerm("BY");
			CACHE = ToTerm("CACHE");
			CALL = ToTerm("CALL");
			CALLBACK = ToTerm("CALLBACK");
			CASCADE = ToTerm("CASCADE");
			CASE = ToTerm("CASE");
			CAST = ToTerm("CAST");
			CHECK = ToTerm("CHECK");
			CONSTANT = ToTerm("CONSTANT");
			CONSTRAINT = ToTerm("CONSTRAINT");
			CREATE = ToTerm("CREATE");
			CURSOR = ToTerm("CURSOR");
			CYCLE = ToTerm("CYCLE");
			DECLARE = ToTerm("DECLARE");
			DELETE = ToTerm("DELETE");
			DEFAULT = ToTerm("DEFAULT");
			EACH = ToTerm("EACH");
			END = ToTerm("END");
			ELSE = ToTerm("ELSE");
			ELSIF = ToTerm("ELSIF");
			EXCEPTION = ToTerm("EXCEPTION");
			EXCEPTION_INIT = ToTerm("EXCEPTION_INIT");
			EXECUTE = ToTerm("EXECUTE");
			EXISTS = ToTerm("EXISTS");
			EXIT = ToTerm("EXIT");
			EXTERNALLY = ToTerm("EXTERNALLY");
			FOR = ToTerm("FOR");
			FOREIGN = ToTerm("FOREIGN");
			GOTO = ToTerm("GOTO");
			GROUPS = ToTerm("GROUPS");
			KEY = ToTerm("KEY");
			IDENTIFIED = ToTerm("IDENTIFIED");
			IDENTITY = ToTerm("IDENTITY");
			IF = ToTerm("IF");
			INCREMENT = ToTerm("INCREMENT");
			INDEX = ToTerm("INDEX");
			INSERT = ToTerm("INSERT");
			IS = ToTerm("IS");
			LOCK = ToTerm("LOCK");
			MAXVALUE = ToTerm("MAXVALUE");
			MINVALUE = ToTerm("MINVALUE");
			NO = ToTerm("NO");
			NOT = ToTerm("NOT");
			NULL = ToTerm("NULL");
			ON = ToTerm("ON");
			OR = ToTerm("OR");
			PASSWORD = ToTerm("PASSWORD");
			PRIMARY = ToTerm("PRIMARY");
			PRAGMA = ToTerm("PRAGMA");
			PROCEDURE = ToTerm("PROCEDURE");
			REPLACE = ToTerm("REPLACE");
			REFERENCES = ToTerm("REFERENCES");
			ROW = ToTerm("ROW");
			SCHEMA = ToTerm("SCHEMA");
			SEQUENCE = ToTerm("SEQUENCE");
			SET = ToTerm("SET");
			START = ToTerm("START");
			TABLE = ToTerm("TABLE");
			THEN = ToTerm("THEN");
			TRIGGER = ToTerm("TRIGGER");
			TYPE = ToTerm("TYPE");
			UPDATE = ToTerm("UPDATE");
			UNIQUE = ToTerm("UNIQUE");
			UNLOCK = ToTerm("UNLOCK");
			USER = ToTerm("USER");
			VIEW = ToTerm("VIEW");
			WHEN = ToTerm("WHEN");
			WITH = ToTerm("WITH");
		}
	}
}
