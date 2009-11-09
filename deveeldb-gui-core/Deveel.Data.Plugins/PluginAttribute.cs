using System;

namespace Deveel.Data.Plugins {
	[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
	public sealed class PluginAttribute : Attribute {
		public PluginAttribute(string name) {
			this.name = name;
		}

		private readonly string name;
		private int order;

		public string Name {
			get { return name; }
		}

		public int Order {
			get { return order; }
			set { order = value; }
		}
	}
}