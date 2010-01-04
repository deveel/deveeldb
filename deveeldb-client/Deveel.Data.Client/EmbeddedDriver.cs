using System;
using System.Reflection;

namespace Deveel.Data.Client {
	internal class EmbeddedDriver : Driver {
		public EmbeddedDriver(object controller, object processor, int queryTimeout) 
			: base(queryTimeout) {
			this.controller = controller;
			this.processor = processor;
			processMethod = processor.GetType().GetMethod("Process", new Type[] {typeof(byte[])});
		}

		private byte[] lastResponse;
		private bool lastResponseRead;
		private readonly object controller;
		private readonly object processor;
		private readonly MethodInfo processMethod;

		public void CreateDatabase(string name, string adminUser, string adminPass) {
			try {
				MethodInfo method = controller.GetType().GetMethod("CreateDatabase");
				method.Invoke(controller, new object[] { null, name, adminUser, adminPass });
			} catch (TargetInvocationException e) {
				throw new DatabaseCreateException(e.InnerException.Message);
			} catch (Exception e) {
				throw new DatabaseCreateException(e.Message);
			}
		}

		#region Overrides of Driver

		protected override void WriteCommand(byte[] command, int offset, int size) {
			byte[] buffer = new byte[size];
			Array.Copy(command, offset, buffer, 0, size);
			lastResponse = (byte[]) processMethod.Invoke(processor, new object[] {buffer});
			lastResponseRead = true;
		}

		protected override byte[] ReadNextCommand(int timeout) {
			while (!lastResponseRead)
				continue;

			lastResponseRead = false;
			return lastResponse;
		}

		protected override void Dispose() {
		}

		#endregion

		public void StartDatabase(string database) {
			try {
				MethodInfo method = controller.GetType().GetMethod("StartDatabase");
				method.Invoke(controller, new object[] { null, database });
			} catch (TargetInvocationException e) {
				throw new DatabaseCreateException(e.InnerException.Message);
			} catch (Exception e) {
				throw new DatabaseCreateException(e.Message);
			}
		}
	}
}