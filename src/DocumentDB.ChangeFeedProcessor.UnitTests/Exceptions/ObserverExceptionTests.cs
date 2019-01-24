//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  Licensed under the MIT license.
//----------------------------------------------------------------

using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.Azure.Documents.ChangeFeedProcessor.Exceptions;
using Xunit;

namespace Microsoft.Azure.Documents.ChangeFeedProcessor.UnitTests.Exceptions
{
    [Trait("Category", "Gated")]
    public class ObserverExceptionTests
    {
        [Fact]
        public void ValidateConstructor()
        {
            Exception exception = new Exception("randomMessage");
            var ex = new ObserverException(exception);
            Assert.Equal(exception.Message, ex.InnerException.Message);
            Assert.Equal(exception, ex.InnerException);
        }

        // Tests the GetObjectData method and the serialization ctor.
        [Fact]
        public void ValidateSerialization_AllFields()
        {
            Exception exception = new Exception("randomMessage");
            var originalException = new ObserverException(exception);
            var buffer = new byte[4096];
            var formatter = new BinaryFormatter();
            var stream1 = new MemoryStream(buffer);
            var stream2 = new MemoryStream(buffer);

            formatter.Serialize(stream1, originalException);
            var deserializedException = (ObserverException)formatter.Deserialize(stream2);

            Assert.Equal(originalException.Message, deserializedException.Message);
            Assert.Equal(originalException.InnerException.Message, deserializedException.InnerException.Message);
        }

        // Make sure that when some fields are not set, serialization is not broken.
        [Fact]
        public void ValidateSerialization_NullFields()
        {
            var originalException = new ObserverException(null);
            var buffer = new byte[4096];
            var formatter = new BinaryFormatter();
            var stream1 = new MemoryStream(buffer);
            var stream2 = new MemoryStream(buffer);

            formatter.Serialize(stream1, originalException);
            var deserializedException = (ObserverException)formatter.Deserialize(stream2);

            Assert.Equal(originalException.Message, deserializedException.Message);
            Assert.Null(deserializedException.InnerException);
        }
    }
}
