using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Routines;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.Security {
	public sealed class SecurityAssertionRegistrar : IDisposable {
		private ICollection<ISecurityAssert> assertions;
		private SecurityAssertionRegistrar parent;
		private bool mutated;
		private IEnumerable<ISecurityAssert> allAssertions;

		internal SecurityAssertionRegistrar(SecurityAssertionRegistrar parent) {
			this.parent = parent;
			assertions = new List<ISecurityAssert>();
		}

		~SecurityAssertionRegistrar() {
			Dispose(false);
		}

		internal IEnumerable<ISecurityAssert> Assertions {
			get {
				var result = allAssertions;
				if (mutated) {
					var list = new List<ISecurityAssert>(assertions);
					if (parent != null)
						list.AddRange(parent.assertions);

					result = allAssertions = list.AsReadOnly();
					mutated = false;
				} else if (result == null) {
					result = new ISecurityAssert[0];
				}

				return result.AsEnumerable();
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (assertions != null) {
					foreach (var assert in assertions) {
						if (assert != null && assert is IDisposable)
							((IDisposable)assert).Dispose();
					}

					assertions.Clear();
				}

				if (allAssertions != null) {
					foreach (var assert in allAssertions) {
						if (assert != null && assert is IDisposable)
							((IDisposable)assert).Dispose();
					}
				}
			}

			parent = null;
			assertions = null;
			allAssertions = null;
		}

		public void Add<T>(T assert) where T : class, ISecurityAssert {
			assertions.Add(assert);
			mutated = true;
		}

		public void Add<T>() where T : class, ISecurityAssert, new() {
			var assert = (T) Activator.CreateInstance(typeof(T));
			Add(assert);
		}

		public void Add(Action<ISecurityContext> assert) {
			Add(new DelegatingSecurityAssert(assert));
		}

		public void Add(Func<ISecurityContext, AssertResult> assert) {
			Add(new DelegatingSecurityAssert(assert));
		}

		public void AddAccess(ObjectName resourceName, DbObjectType resourceType, Privileges privileges) {
			Add(new ResourceAccessSecurityAssert(resourceName, resourceType, privileges));
		}

		public void AddCreate(ObjectName resourceName, DbObjectType resourceType) {
			AddAccess(resourceName, resourceType, Privileges.Create);
		}

		public void AddDrop(ObjectName resourceName, DbObjectType resourceType) {
			AddAccess(resourceName, resourceType, Privileges.Drop);
		}

		public void AddAlter(ObjectName resourceName, DbObjectType resourceType) {
			AddAccess(resourceName, resourceType, Privileges.Alter);
		}

		public void AddSelect(ObjectName tableName, DbObjectType resourceType) {
			AddAccess(tableName, resourceType, Privileges.Select);
		}

		public void AddReference(ObjectName resourceName, DbObjectType resourceType) {
			AddAccess(resourceName, resourceType, Privileges.References);
		}


		public void AddSelect(IQueryPlanNode queryPlan) {
			// TODO: the current implementation returns table names and view names with no distinction
			var accessedResources = queryPlan.DiscoverAccessedResources();
			foreach (var resource in accessedResources) {
				AddSelect(resource.ResourceName, resource.ResourceType);
			}
		}

		public void AddUpdate(ObjectName tableName) {
			AddAccess(tableName, DbObjectType.Table, Privileges.Update);
		}

		public void AddInsert(ObjectName tableName) {
			AddAccess(tableName, DbObjectType.Table, Privileges.Insert);
		}

		public void AddDelete(ObjectName tableName) {
			AddAccess(tableName, DbObjectType.Table, Privileges.Delete);
		}

		public void AddExecute(ObjectName resourceName, params InvokeArgument[] arguments) {
			Add(new ResourceAccessSecurityAssert(resourceName, arguments));
		}

		#region DelegatingSecurityAssert

		class DelegatingSecurityAssert : ISecurityAssert, IDisposable {
			public DelegatingSecurityAssert(Action<ISecurityContext> assertAction) {
				AssertAction = assertAction;
			}

			public DelegatingSecurityAssert(Func<ISecurityContext, AssertResult> assertFunc) {
				AssertFunc = assertFunc;
			}

			public Action<ISecurityContext> AssertAction { get; private set; }

			public Func<ISecurityContext, AssertResult> AssertFunc { get; private set; }

			public AssertResult Assert(ISecurityContext context) {
				try {
					if (AssertFunc != null)
						return AssertFunc(context);

					AssertAction(context);
					return AssertResult.Allow();
				} catch (Exception ex) {
					return AssertResult.Deny(ex);
				}
			}

			public void Dispose() {
				AssertAction = null;
				AssertFunc = null;
			}
		}

		#endregion
	}
}
