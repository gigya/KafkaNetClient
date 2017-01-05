using System;

namespace KafkaClient.Common
{
    public class Disposable : IDisposable
    {
        private readonly Action _disposeAction;

        public Disposable(Action disposeAction)
        {
            _disposeAction = disposeAction;
        }

        public void Dispose()
        {
            _disposeAction();
        }

        public static IDisposable None { get; } = new Disposable(() => { });
    }
}