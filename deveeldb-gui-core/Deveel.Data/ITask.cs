using System;

namespace Deveel.Data {
	public interface ITask {
		bool IsBusy { get; }


		void Execute();

		void Cancel();
	}
}