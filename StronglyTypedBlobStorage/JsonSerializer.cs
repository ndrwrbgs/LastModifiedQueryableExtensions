namespace StronglyTypedBlobStorage {
    using System.IO;
    using System.Text;
    using System.Threading.Tasks;

    using Newtonsoft.Json;

    public sealed class JsonSerializer<T> : ISerializer<T>
    {
        private readonly JsonSerializer jsonSerializer;

        public JsonSerializer()
            : this (new JsonSerializer())
        {
        }

        public JsonSerializer(
            JsonSerializer serializer)
        {
            this.jsonSerializer = serializer;
        }

        public async Task<T> Deserialize(Stream fromStream)
        {
            await Task.Yield();
            using (TextReader textReader = new StreamReader(fromStream))
            {
                using (JsonReader reader = new JsonTextReader(textReader))
                {
                    return this.jsonSerializer.Deserialize<T>(reader);
                }
            }
        }

        public Stream Serialize(T item)
        {
            var stream = new MemoryStream();

            using (TextWriter textWriter = new StreamWriter(stream, Encoding.UTF8, 1024, leaveOpen: true))
            {
                this.jsonSerializer.Serialize(textWriter, item, typeof(T));
            }

            stream.Position = 0;

            return stream;
        }
    }
}