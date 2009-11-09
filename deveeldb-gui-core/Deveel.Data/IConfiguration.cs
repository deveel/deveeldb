using System;
using System.ComponentModel;

namespace Deveel.Data {
	public interface IConfiguration : INotifyPropertyChanged {
		bool HasChanges { get; }

		string Name { get; }

		object Context { get; }


		void Save();
	}
}