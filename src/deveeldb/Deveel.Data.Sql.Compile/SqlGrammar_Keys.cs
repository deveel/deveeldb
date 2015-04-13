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

namespace Deveel.Data.Sql.Compile {
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
		private KeyTerm BIGINT;
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
		private KeyTerm CHAR;
		private KeyTerm CHECK;
		private KeyTerm CLOB;
		private KeyTerm CONSTANT;
		private KeyTerm CONSTRAINT;
		private KeyTerm CREATE;
		private KeyTerm CURSOR;
		private KeyTerm CYCLE;
		private KeyTerm DATE;
		private KeyTerm DAY;
		private KeyTerm DECIMAL;
		private KeyTerm DECLARE;
		private KeyTerm DEFAULT;
		private KeyTerm DELETE;
		private KeyTerm DESC;
		private KeyTerm DISTINCT;
		private KeyTerm DOUBLE;
		private KeyTerm EACH;
		private KeyTerm END;
		private KeyTerm ELSE;
		private KeyTerm ELSIF;
		private KeyTerm EXCEPT;
		private KeyTerm EXCEPTION;
		private KeyTerm EXCEPTION_INIT;
		private KeyTerm EXECUTE;
		private KeyTerm EXISTS;
		private KeyTerm EXIT;
		private KeyTerm EXTERNALLY;
		private KeyTerm FALSE;
		private KeyTerm FLOAT;
		private KeyTerm FOR;
		private KeyTerm FROM;
		private KeyTerm FOREIGN;
		private KeyTerm GEOMETRY;
		private KeyTerm GOTO;
		private KeyTerm GROUP;
		private KeyTerm GROUPS;
		private KeyTerm HAVING;
		private KeyTerm KEY;
		private KeyTerm JOIN;
		private KeyTerm IDENTIFIED;
		private KeyTerm IDENTITY;
		private KeyTerm IF;
		private KeyTerm IN;
		private KeyTerm INCREMENT;
		private KeyTerm INDEX;
		private KeyTerm INNER;
		private KeyTerm INSERT;
		private KeyTerm INT;
		private KeyTerm INTEGER;
		private KeyTerm INTERSECT;
		private KeyTerm INTERVAL;
		private KeyTerm INTO;
		private KeyTerm IS;
		private KeyTerm LEFT;
		private KeyTerm LIKE;
		private KeyTerm LOCALE;
		private KeyTerm LOCK;
		private KeyTerm LONG;
		private KeyTerm MAXVALUE;
		private KeyTerm MINUS;
		private KeyTerm MINVALUE;
		private KeyTerm MONTH;
		private KeyTerm NO;
		private KeyTerm NOT;
		private KeyTerm NULL;
		private KeyTerm NUMBER;
		private KeyTerm NUMERIC;
		private KeyTerm ON;
		private KeyTerm OR;
		private KeyTerm ORDER;
		private KeyTerm OUTER;
		private KeyTerm PASSWORD;
		private KeyTerm PRIMARY;
		private KeyTerm PRAGMA;
		private KeyTerm PROCEDURE;
		private KeyTerm REAL;
		private KeyTerm REFERENCES;
		private KeyTerm REPLACE;
		private KeyTerm RIGHT;
		private KeyTerm ROW;
		private KeyTerm ROWTYPE;
		private KeyTerm SCHEMA;
		private KeyTerm SECOND;
		private KeyTerm SELECT;
		private KeyTerm SEQUENCE;
		private KeyTerm SET;
		private KeyTerm SMALLINT;
		private KeyTerm START;
		private KeyTerm TABLE;
		private KeyTerm THEN;
		private KeyTerm TIME;
		private KeyTerm TIMESTAMP;
		private KeyTerm TINYINT;
		private KeyTerm TO;
		private KeyTerm TRIGGER;
		private KeyTerm TRUE;
		private KeyTerm TYPE;
		private KeyTerm UPDATE;
		private KeyTerm UNION;
		private KeyTerm UNIQUE;
		private KeyTerm UNLOCK;
		private KeyTerm USER;
		private KeyTerm VARBINARY;
		private KeyTerm VARCHAR;
		private KeyTerm VIEW;
		private KeyTerm YEAR;
		private KeyTerm WHEN;
		private KeyTerm WHERE;
		private KeyTerm WITH;
		private KeyTerm XML;

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
			BIGINT = ToTerm("BIGINT");
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
			CHAR = ToTerm("CHAR");
			CHECK = ToTerm("CHECK");
			CLOB = ToTerm("CLOB");
			CONSTANT = ToTerm("CONSTANT");
			CONSTRAINT = ToTerm("CONSTRAINT");
			CREATE = ToTerm("CREATE");
			CURSOR = ToTerm("CURSOR");
			CYCLE = ToTerm("CYCLE");
			DATE = ToTerm("DATE");
			DAY = ToTerm("DAY");
			DECIMAL = ToTerm("DECIMAL");
			DECLARE = ToTerm("DECLARE");
			DELETE = ToTerm("DELETE");
			DEFAULT = ToTerm("DEFAULT");
			DESC = ToTerm("DESC");
			DISTINCT = ToTerm("DISTINCT");
			DOUBLE = ToTerm("DOUBLE");
			EACH = ToTerm("EACH");
			END = ToTerm("END");
			ELSE = ToTerm("ELSE");
			ELSIF = ToTerm("ELSIF");
			EXCEPT = ToTerm("EXCEPT");
			EXCEPTION = ToTerm("EXCEPTION");
			EXCEPTION_INIT = ToTerm("EXCEPTION_INIT");
			EXECUTE = ToTerm("EXECUTE");
			EXISTS = ToTerm("EXISTS");
			EXIT = ToTerm("EXIT");
			EXTERNALLY = ToTerm("EXTERNALLY");
			FALSE = ToTerm("FALSE");
			FLOAT = ToTerm("FLOAT");
			FOR = ToTerm("FOR");
			FROM = ToTerm("FROM");
			FOREIGN = ToTerm("FOREIGN");
			GEOMETRY = ToTerm("GEOMETRY");
			GOTO = ToTerm("GOTO");
			GROUP = ToTerm("GROUP");
			GROUPS = ToTerm("GROUPS");
			HAVING = ToTerm("HAVING");
			KEY = ToTerm("KEY");
			JOIN = ToTerm("JOIN");
			IDENTIFIED = ToTerm("IDENTIFIED");
			IDENTITY = ToTerm("IDENTITY");
			IF = ToTerm("IF");
			IN = ToTerm("IN");
			INCREMENT = ToTerm("INCREMENT");
			INDEX = ToTerm("INDEX");
			INNER = ToTerm("INNER");
			INSERT = ToTerm("INSERT");
			INT = ToTerm("INT");
			INTEGER = ToTerm("INTEGER");
			INTERSECT = ToTerm("INTERSECT");
			INTERVAL = ToTerm("INTERVAL");
			INTO = ToTerm("INTO");
			IS = ToTerm("IS");
			LEFT = ToTerm("LEFT");
			LIKE = ToTerm("LIKE");
			LOCALE = ToTerm("LOCALE");
			LOCK = ToTerm("LOCK");
			LONG = ToTerm("LONG");
			MAXVALUE = ToTerm("MAXVALUE");
			MINUS = ToTerm("MINUS");
			MINVALUE = ToTerm("MINVALUE");
			MONTH = ToTerm("MONTH");
			NO = ToTerm("NO");
			NOT = ToTerm("NOT");
			NULL = ToTerm("NULL");
			NUMBER = ToTerm("NUMBER");
			NUMERIC = ToTerm("NUMERIC");
			ON = ToTerm("ON");
			OR = ToTerm("OR");
			ORDER = ToTerm("ORDER");
			OUTER = ToTerm("OUTER");
			PASSWORD = ToTerm("PASSWORD");
			PRIMARY = ToTerm("PRIMARY");
			PRAGMA = ToTerm("PRAGMA");
			PROCEDURE = ToTerm("PROCEDURE");
			REAL = ToTerm("REAL");
			REPLACE = ToTerm("REPLACE");
			REFERENCES = ToTerm("REFERENCES");
			RIGHT = ToTerm("RIGHT");
			ROW = ToTerm("ROW");
			ROWTYPE = ToTerm("ROWTYPE");
			SCHEMA = ToTerm("SCHEMA");
			SECOND = ToTerm("SECOND");
			SELECT = ToTerm("SELECT");
			SEQUENCE = ToTerm("SEQUENCE");
			SET = ToTerm("SET");
			SMALLINT = ToTerm("SMALLINT");
			START = ToTerm("START");
			TABLE = ToTerm("TABLE");
			THEN = ToTerm("THEN");
			TIME = ToTerm("TIME");
			TIMESTAMP = ToTerm("TIMESTAMP");
			TINYINT = ToTerm("TINYINT");
			TO = ToTerm("TO");
			TRIGGER = ToTerm("TRIGGER");
			TRUE = ToTerm("TRUE");
			TYPE = ToTerm("TYPE");
			UPDATE = ToTerm("UPDATE");
			UNION = ToTerm("UNION");
			UNIQUE = ToTerm("UNIQUE");
			UNLOCK = ToTerm("UNLOCK");
			USER = ToTerm("USER");
			VARBINARY = ToTerm("VARBINARY");
			VARCHAR = ToTerm("VARCHAR");
			VIEW = ToTerm("VIEW");
			WHEN = ToTerm("WHEN");
			WHERE = ToTerm("WHERE");
			WITH = ToTerm("WITH");
			YEAR = ToTerm("YEAR");
			XML = ToTerm("XML");
		}
	}
}
