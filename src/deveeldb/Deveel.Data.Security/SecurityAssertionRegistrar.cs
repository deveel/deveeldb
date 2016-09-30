using System;
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Security {
	public sealed class SecurityAssertionRegistrar : IEnumerable<ISecurityAssert>, IDisposable {
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

		IEnumerator<ISecurityAssert> IEnumerable<ISecurityAssert>.GetEnumerator() {
			if (mutated) {
				var list = new List<ISecurityAssert>(assertions);
				if (parent != null)
					list.AddRange(parent.assertions);

				allAssertions = list.AsReadOnly();
				mutated = false;
			}

			return allAssertions.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return (this as IEnumerable<ISecurityAssert>).GetEnumerator();
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
			}

			assertions = null;
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

		#region DelegatingSecurityAssert

		class DelegatingSecurityAssert : ISecurityAssert {
			public DelegatingSecurityAssert(Action<ISecurityContext> assertAction) {
				AssertAction = assertAction;
			}

			public Action<ISecurityContext> AssertAction { get; private set; }

			public AssertResult Assert(ISecurityContext context) {
				try {
					AssertAction(context);
					return AssertResult.Allow();
				} catch (Exception ex) {
					return AssertResult.Deny(ex);
				}
			}
		}

		#endregion
	}
}
