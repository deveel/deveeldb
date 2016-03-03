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
using System.Collections.ObjectModel;

using Deveel.Data.Serialization;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public abstract class CodeBlock : ISerializable, ISqlCodeObject, IExecutable, IDisposable {
		internal CodeBlock() {
			Objects = new SqlObjectCollection(this);
		}

		internal CodeBlock(ObjectData data) {
			Label = data.GetString("Label");
			Objects = DeserializeObjects(data);
		}

		~CodeBlock() {
			Dispose(false);
		}

		public string Label { get; set; }

		public ICollection<ISqlCodeObject> Objects { get; private set; }

		void ISerializable.GetData(SerializeData data) {
			data.SetValue("Label", Label);
			SerializeObjects(data);

			GetData(data);
		}

		private void SerializeObjects(SerializeData data) {
			throw new NotImplementedException();
		}

		void IExecutable.Execute(ExecutionContext context) {
			Execute(context);
		}

		protected virtual void Execute(ExecutionContext context) {
			foreach (var obj in Objects) {
				if (obj is IExecutable) {
					(obj as IExecutable).Execute(context);
				}
			}
		}

		protected virtual void GetData(SerializeData data) {
			
		}

		private ICollection<ISqlCodeObject> DeserializeObjects(ObjectData data) {
			throw new NotImplementedException();
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				if (Objects != null)
					Objects.Clear();
			}

			Objects = null;
		}

		void IDisposable.Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void AssertPlSqlStatement(ISqlCodeObject obj) {
			if (obj is IStatement &&
				!(obj is IPlSqlStatement)) {
				throw new ArgumentException(String.Format("The statement of type '{0}' cannot be inserted into a PL/SQL block.",
					obj.GetType()));
			}
		}

		private void AssertNotLoopControl(ISqlCodeObject obj) {
			if (obj is LoopControlStatement) {
				//TODO: Check the tree and see if we are in a loop context...
			}
		}

		private void AssertAllowedObject(ISqlCodeObject obj) {
			AssertPlSqlStatement(obj);
			AssertNotLoopControl(obj);
		}

		#region SqlObjectCollection

		class SqlObjectCollection : Collection<ISqlCodeObject> {
			private readonly CodeBlock block;

			public SqlObjectCollection(CodeBlock block) {
				this.block = block;
			}

			private void AssertPlSqlObject(ISqlCodeObject obj) {
				block.AssertAllowedObject(obj);
			}

			protected override void InsertItem(int index, ISqlCodeObject item) {
				AssertPlSqlObject(item);
				base.InsertItem(index, item);
			}

			protected override void SetItem(int index, ISqlCodeObject item) {
				AssertPlSqlObject(item);
				base.SetItem(index, item);
			}
		}

		#endregion

	}
}
