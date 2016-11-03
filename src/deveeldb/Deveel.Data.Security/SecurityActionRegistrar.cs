// 
//  Copyright 2010-2016 Deveel
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

using Deveel.Data.Sql;

namespace Deveel.Data.Security {
	public sealed class SecurityActionRegistrar : IDisposable {
		private ICollection<ISecurityPostExecuteAction> postExecute;
		private ICollection<ISecurityBeforeExecuteAction> beforeExecute;

		internal SecurityActionRegistrar() {
			postExecute = new List<ISecurityPostExecuteAction>();
			beforeExecute = new List<ISecurityBeforeExecuteAction>();
		}

		~SecurityActionRegistrar() {
			Dispose(false);
		}

		public void Add<T>(T action) where T : ISecurityAction {
			if (action == null)
				throw new ArgumentNullException("action");

			if (action is ISecurityBeforeExecuteAction) {
				beforeExecute.Add((ISecurityBeforeExecuteAction) action);
			} else if (action is ISecurityPostExecuteAction) {
				postExecute.Add((ISecurityPostExecuteAction)action);
			} else {
				throw new ArgumentException(String.Format("Action of type '{0}' is not supported.", action.GetType()));
			}
		}

		public void Add<T>() where T : class, ISecurityAction, new() {
			var action = (T) Activator.CreateInstance<T>();
			Add(action);
		}

		public void AddResourceGrant(ObjectName resourceName, DbObjectType resourceType, Privileges privileges) {
			Add(new ResourceGrantAction(resourceName, resourceType, privileges));
		}

		internal void AfterExecute(ISecurityContext context) {
			foreach (var action in postExecute) {
				action.OnActionExecuted(context);
			}
		}

		internal void BeforeExecute(ISecurityContext context) {
			foreach (var action in beforeExecute) {
				action.OnExecuteAction(context);
			}
		}

		void IDisposable.Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (postExecute != null) {
					foreach (var action in postExecute) {
						if (action is IDisposable)
							((IDisposable)action).Dispose();
					}

					postExecute.Clear();
				}

				if (beforeExecute != null) {
					foreach (var action in beforeExecute) {
						if (action is IDisposable)
							((IDisposable)action).Dispose();
					}

					beforeExecute.Clear();
				}
			}

			postExecute = null;
			beforeExecute = null;
		}
	}
}
