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

namespace Deveel.Data.DbSystem {
	public sealed partial class DatabaseConnection {
		/// <summary>
		/// Requests the sequence generator for the next value.
		/// </summary>
		/// <param name="name"></param>
		/// <remarks>
		/// <b>Note:</b> This does <b>note</b> check that the user owning 
		/// the session has the correct privileges to perform the operation.
		/// </remarks>
		/// <returns></returns>
		public long NextSequenceValue(String name) {
			// Resolve and ambiguity test
			TableName seq_name = ResolveToTableName(name);
			return Transaction.NextSequenceValue(seq_name);
		}

		/// <summary>
		/// Returns the current sequence value for the given sequence generator.
		/// </summary>
		/// <param name="name"></param>
		/// <remarks>
		/// The value returned is the same value returned by <see cref="NextSequenceValue"/>.
		/// <para>
		/// <b>Note:</b> This does <b>note</b> check that the user owning 
		/// the session has the correct privileges to perform the operation.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		/// <exception cref="StatementException">
		/// If no value was returned by <see cref="NextSequenceValue"/>.
		/// </exception>
		public long LastSequenceValue(String name) {
			// Resolve and ambiguity test
			TableName seq_name = ResolveToTableName(name);
			return Transaction.LastSequenceValue(seq_name);
		}

		/// <summary>
		/// Sets the sequence value for the given sequence generator.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="value"></param>
		/// <remarks>
		/// <b>Note:</b> This does <b>note</b> check that the user owning 
		/// the session has the correct privileges to perform the operation.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If the generator does not exist or it is not possible to set the 
		/// value for the generator.
		/// </exception>
		public void SetSequenceValue(String name, long value) {
			// Resolve and ambiguity test
			TableName seq_name = ResolveToTableName(name);
			Transaction.SetSequenceValue(seq_name, value);
		}

		/// <summary>
		/// Returns the next unique identifier for the given table from 
		/// the schema.
		/// </summary>
		/// <param name="name"></param>
		/// <returns></returns>
		public long NextUniqueID(TableName name) {
			return Transaction.NextUniqueID(name);
		}

		/// <summary>
		/// Returns the next unique identifier for the given table from 
		/// the current schema.
		/// </summary>
		/// <param name="table_name"></param>
		/// <returns></returns>
		public long NextUniqueID(String table_name) {
			TableName tname = TableName.Resolve(currentSchema, table_name);
			return NextUniqueID(tname);
		}

		/// <summary>
		/// Returns the current unique identifier for the given table from
		/// the current schema.
		/// </summary>
		/// <param name="table_name"></param>
		/// <returns></returns>
		public long CurrentUniqueID(TableName table_name) {
			return Transaction.CurrentUniqueID(table_name);
		}

		public long CurrentUniqueID(string table_name) {
			return CurrentUniqueID(TableName.Resolve(currentSchema, table_name));
		}

		/// <summary>
		/// Creates a new sequence generator with the given name and initializes 
		/// it with the given details.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="startValue"></param>
		/// <param name="incrementBy"></param>
		/// <param name="minValue"></param>
		/// <param name="maxValue"></param>
		/// <param name="cache"></param>
		/// <param name="cycle"></param>
		/// <remarks>
		/// This does <b>not</b> check if the given name clashes with an 
		/// existing database object.
		/// </remarks>
		public void CreateSequenceGenerator(TableName name, long startValue, long incrementBy, long minValue, long maxValue, long cache, bool cycle) {

			// Check the name of the database object isn't reserved (OLD/NEW)
			CheckAllowCreate(name);

			Transaction.CreateSequenceGenerator(name, startValue, incrementBy, minValue, maxValue, cache, cycle);
		}

		/// <summary>
		/// Drops an existing sequence generator with the given name.
		/// </summary>
		/// <param name="name"></param>
		public void DropSequenceGenerator(TableName name) {
			Transaction.DropSequenceGenerator(name);
		}
	}
}