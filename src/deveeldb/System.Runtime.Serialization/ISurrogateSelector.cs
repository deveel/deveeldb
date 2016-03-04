using System;

namespace System.Runtime.Serialization {
	public interface ISurrogateSelector {
		void ChainSelector(ISurrogateSelector selector);

		ISurrogateSelector GetNextSelector();

		ISerializationSurrogate GetSurrogate(Type type, StreamingContext context, out ISurrogateSelector selector);
	}
}
