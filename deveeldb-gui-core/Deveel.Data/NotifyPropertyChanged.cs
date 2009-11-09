using System;
using System.ComponentModel;

namespace Deveel.Data {
	public abstract class NotifyPropertyChanged : INotifyPropertyChanged {
		public event PropertyChangedEventHandler PropertyChanged;

		protected void OnPropertyChanged(string propertyName) {
			if (PropertyChanged != null) {
				PropertyChanged(this, new PropertyChangedEventArgs(propertyName));
			}
		}

		protected void OnPropertyChanged(PropertyChangedEventArgs e) {
			if (PropertyChanged != null)
				PropertyChanged(this, e);
		}
	}
}