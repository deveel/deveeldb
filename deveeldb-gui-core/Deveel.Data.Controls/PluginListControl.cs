using System;
using System.Collections;
using System.Windows.Forms;

using Deveel.Data.Plugins;

namespace Deveel.Data.Deveel.Data.Controls {
	public partial class PluginListControl : UserControl {
		public PluginListControl() {
			InitializeComponent();

			plugins = new PluginCollection(this);
		}

		private readonly PluginCollection plugins;

		public IList Plugins {
			get { return plugins; }
		}

		private class PluginCollection : ArrayList {
			public PluginCollection(PluginListControl control) {
				this.control = control;
			}

			private readonly PluginListControl control;

			public override object this[int index] {
				get {
					//TODO:
					return base[index];
				}
				set {
					//TODO:
					base[index] = value;
				}
			}

			public override int Add(object value) {
				IPlugin plugin = (IPlugin) value;

				control.listView1.Items.Add(
					new ListViewItem(new string[] {plugin.Name, plugin.Description, plugin.GetType().AssemblyQualifiedName}));

				return base.Add(value);
			}

			public override void Remove(object obj) {
				IPlugin plugin = (IPlugin) obj;
				foreach (ListViewItem item in control.listView1.Items) {
					//TODO:
				}

				base.Remove(obj);
			}
		}
	}
}
