using System;
using System.ComponentModel;

namespace Deveel.Data.Plugins {
	public sealed class CoreConfiguration : NotifyPropertyChanged, IConfiguration {
		public CoreConfiguration(ISettings settings) {
			this.settings = settings;
			settingsWrapper = new CoreSettingsWrapper(settings);
			settingsWrapper.PropertyChanged += new PropertyChangedEventHandler(settingsWrapper_PropertyChanged);
		}

		private bool changed;
		private readonly ISettings settings;
		private readonly CoreSettingsWrapper settingsWrapper;

		public bool HasChanges {
			get { return changed; }
		}

		public string Name {
			get { return "Client Settings"; }
		}

		public object Context {
			get { return settingsWrapper; }
		}

		void settingsWrapper_PropertyChanged(object sender, PropertyChangedEventArgs e) {
			SetChanged(true);
		}

		private void SetChanged(bool value) {
			if (changed != value) {
				changed = value;
				OnPropertyChanged("HasChanges");
			}
		}

		public void Save() {
			settings.SetProperty(SettingsProperties.EnableBatching, settingsWrapper.EnableQueryBatches);
			settings.SetProperty(SettingsProperties.PluginFileFilter, settingsWrapper.PlugInFileFilter);
			settings.SetProperty(SettingsProperties.LoadPlugins, settingsWrapper.LoadExternalPlugins);
			settings.SetProperty(SettingsProperties.DateTimeFormat, settingsWrapper.DateTimeFormat);
			settings.SetProperty(SettingsProperties.NullString, settingsWrapper.NullText);
			settings.SetProperty(SettingsProperties.ConnectionStringsFile, settingsWrapper.ConnectionFileName);
			SetChanged(false);
		}

		private class CoreSettingsWrapper : NotifyPropertyChanged {
			public CoreSettingsWrapper(ISettings settings) {
				EnableQueryBatches = (bool) settings.GetProperty(SettingsProperties.EnableBatching);
				PlugInFileFilter = (string) settings.GetProperty(SettingsProperties.PluginFileFilter);
				LoadExternalPlugins = (bool) settings.GetProperty(SettingsProperties.LoadPlugins);
				DateTimeFormat = (string) settings.GetProperty(SettingsProperties.DateTimeFormat);
				NullText = (string) settings.GetProperty(SettingsProperties.NullString);
				ConnectionFileName = (string) settings.GetProperty(SettingsProperties.ConnectionStringsFile);
			}

			private bool enableQueryBatches;
			private string pluginFileFilter;
			private bool loadPlugins;
			private string nullText;
			private string dateTimeFormat;
			private string connFileName;

			[Category("Query")]
			[Description("Set to true to enable the batches feature for queries passed to the server.")]
			[DefaultValue(true)]
			public bool EnableQueryBatches {
				get { return enableQueryBatches; }
				set {
					if (enableQueryBatches != value) {
						enableQueryBatches = value;
						OnPropertyChanged("EnableQueryBatches");
					}
				}
			}

			[Category("Plugins")]
			[Description("The file filter used for finding plugins (*.Plugin.dll)")]
			[DefaultValue("*.Plugin.dll")]
			public string PlugInFileFilter {
				get { return pluginFileFilter; }
				set {
					if (pluginFileFilter != value) {
						pluginFileFilter = value;
						OnPropertyChanged("PluginFileFilter");
					}
				}
			}

			[Category("Plugins")]
			[Description("If true, external plugin files will be loaded (requires restart).")]
			[DefaultValue(true)]
			public bool LoadExternalPlugins {
				get { return loadPlugins; }
				set {
					if (loadPlugins != value) {
						loadPlugins = value;
						OnPropertyChanged("LoadExternalPlugins");
					}
				}
			}

			[Category("Query")]
			[Description("The string format used to represent the TIME values obtained by querying a database. (yyyy-MM-dd HH:mm:ss.zz)")]
			[DefaultValue("yyyy-MM-dd HH:mm:ss.zz")]
			public string DateTimeFormat {
				get { return dateTimeFormat; }
				set {
					if (dateTimeFormat != value) {
						dateTimeFormat = value;
						OnPropertyChanged("DateTimeFormat");
					}
				}
			}

			[Category("Query")]
			[Description("The string values used to represent NULL values obtained by querying a database. (<NULL>)")]
			[DefaultValue("<NULL>")]
			public string NullText {
				get { return nullText; }
				set {
					if (nullText != value) {
						nullText = value;
						OnPropertyChanged("NullText");
					}
				}
			}

			[Category("Application")]
			[Description("The file containing the stored connection strings.")]
			[DefaultValue("connections.xml")]
			public string ConnectionFileName {
				get { return connFileName; }
				set {
					if (connFileName != value) {
						connFileName = value;
						OnPropertyChanged("ConnectionFileName");
					}
				}
			}
		}
	}
}