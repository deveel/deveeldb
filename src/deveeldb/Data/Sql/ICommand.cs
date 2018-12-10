using System;

using Deveel.Data.Events;

namespace Deveel.Data.Sql {
	public interface ICommand : IContext, IEventSource {
		ISession Session { get; }

		SqlCommand SourceCommand { get; }
	}
}