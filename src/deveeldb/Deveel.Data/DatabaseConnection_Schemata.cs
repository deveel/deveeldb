// 
//  Copyright 2010-2011  Deveel
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

using System;

namespace Deveel.Data {
	public sealed partial class DatabaseConnection {
		/// <summary>
		/// The name of the schema that this connection is currently in.  If the
		/// schema is "" then this connection is in the default schema (effectively
		/// no schema).
		/// </summary>
		private string current_schema;

		/// <summary>
		/// Gets or sets the name of the schema that this connection is within.
		/// </summary>
		public string CurrentSchema {
			get { return current_schema; }
			set { current_schema = value; }
		}

		/// <summary>
		/// Changes the default schema to the given schema.
		/// </summary>
		/// <param name="schema_name"></param>
		public void SetDefaultSchema(String schema_name) {
			bool ignore_case = IsInCaseInsensitiveMode;
			SchemaDef schema = ResolveSchemaCase(schema_name, ignore_case);
			if (schema == null)
				throw new ApplicationException("Schema '" + schema_name + "' does not exist.");

			// Set the default schema for this connection
			CurrentSchema = schema.Name;
		}

		/// <inheritdoc cref="Data.Transaction.CreateSchema"/>
		public void CreateSchema(String name, String type) {
			// Assert
			CheckExclusive();
			Transaction.CreateSchema(name, type);
		}

		/// <inheritdoc cref="Data.Transaction.DropSchema"/>
		public void DropSchema(String name) {
			// Assert
			CheckExclusive();
			Transaction.DropSchema(name);
		}

		/// <inheritdoc cref="Data.Transaction.SchemaExists"/>
		public bool SchemaExists(String name) {
			return Transaction.SchemaExists(name);
		}

		/// <inheritdoc cref="Data.Transaction.ResolveSchemaCase"/>
		public SchemaDef ResolveSchemaCase(String name, bool ignore_case) {
			return Transaction.ResolveSchemaCase(name, ignore_case);
		}

		/**
		 * Convenience - returns the SchemaDef object given the name of the schema.
		 * If identifiers are case insensitive, we resolve the case of the schema
		 * name also.
		 */
		///<summary>
		///</summary>
		///<param name="name"></param>
		///<returns></returns>
		public SchemaDef ResolveSchemaName(String name) {
			bool ignore_case = IsInCaseInsensitiveMode;
			return ResolveSchemaCase(name, ignore_case);
		}

		/// <inheritdoc cref="Data.Transaction.GetSchemaList"/>
		public SchemaDef[] GetSchemaList() {
			return Transaction.GetSchemaList();
		}
	}
}