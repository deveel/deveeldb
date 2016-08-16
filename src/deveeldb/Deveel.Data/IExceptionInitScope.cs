using System;

namespace Deveel.Data {
	public interface IExceptionInitScope {
		void DeclareException(int errorCode, string exceptionName);

		DeclaredException FindExceptionByName(string name);
	}
}
