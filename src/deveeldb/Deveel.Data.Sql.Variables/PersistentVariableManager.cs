using System;

using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Variables {
	public sealed class PersistentVariableManager : VariableManager, IObjectManager {
		public PersistentVariableManager(ITransaction transaction)
			: base(transaction) {
			Transaction = transaction;
		}

		public ITransaction Transaction { get; private set; }

		protected override void Dispose(bool disposing) {
			Transaction = null;
			base.Dispose(disposing);
		}


		DbObjectType IObjectManager.ObjectType {
			get { return DbObjectType.Variable; }
		}

		void IObjectManager.Create() {
			//TODO:
		}

		void IObjectManager.CreateObject(IObjectInfo objInfo) {
			var variableInfo = objInfo as VariableInfo;
			if (variableInfo == null)
				throw new ArgumentException();

			DefineVariable(variableInfo);
		}

		protected override void OnDefineVariable(Variable variable) {
			throw new NotImplementedException();
		}

		bool IObjectManager.RealObjectExists(ObjectName objName) {
			return (this as IObjectManager).ObjectExists(objName);
		}

		bool IObjectManager.ObjectExists(ObjectName objName) {
			if (objName == null)
				throw new ArgumentNullException("objName");

			if (objName.Parent != null)
				throw new ArgumentException();

			return VariableExists(objName.Name);
		}

		public override bool VariableExists(string name) {
			return VariableExists(name, Transaction.IgnoreIdentifiersCase());
		}

		IDbObject IObjectManager.GetObject(ObjectName objName) {
			if (objName == null)
				throw new ArgumentNullException("objName");

			if (objName.Parent != null)
				throw new ArgumentException();

			return GetVariable(objName.Name);
		}

		protected override Variable OnGetVariable(string name) {
			// TODO:
			return null;
		}

		bool IObjectManager.AlterObject(IObjectInfo objInfo) {
			throw new NotSupportedException();
		}

		bool IObjectManager.DropObject(ObjectName objName) {
			if (objName == null)
				throw new ArgumentNullException("objName");

			if (objName.Parent != null)
				throw new ArgumentException();

			return DropVariable(objName.Name);
		}

		protected override bool OnDropVariable(string name, out Variable variable) {
			throw new NotImplementedException();
		}

		ObjectName IObjectManager.ResolveName(ObjectName objName, bool ignoreCase) {
			if (objName.Parent != null)
				return null;

			if (VariableExists(objName.Name, ignoreCase))
				return new ObjectName(objName.Name);

			return null;
		}

		private bool VariableExists(string name, bool ignoreCase) {
			throw new NotImplementedException();
		}
	}
}
