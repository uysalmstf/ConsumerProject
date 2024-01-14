using System;
using Confluent.Kafka;
using Newtonsoft.Json;
using Npgsql;

class Program
{
    static void Main()
    {
        string connectionString = "Host=localhost;Port=5432;Username=mustafa;Password=102030;Database=TokenMinted";
        var connection = new NpgsqlConnection(connectionString);
        using (connection)
        {
            try
            {
                // Bağlantıyı aç
                connection.Open();

                // PostgreSQL sorgu yazma
                var config = new ConsumerConfig
                {
                    BootstrapServers = "localhost:29092", // Kafka broker adresi
                    GroupId = "deneme",       // Tüketici grubu adı
                    AutoOffsetReset = AutoOffsetReset.Earliest, // Başlangıçta en eski mesajdan itibaren okuma
                };

                using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
                {
                    consumer.Subscribe("TokenMintedNew"); // Okunacak Kafka topic adı

                    try
                    {
                        while (true)
                        {
                            var consumeResult = consumer.Consume();

                            if (consumeResult != null)
                            {
                                Console.WriteLine($"Received message: {consumeResult.Message.Value}");
                                MinterObj minterObj = JsonConvert.DeserializeObject<MinterObj>(consumeResult.Message.Value);

                                string sql = "INSERT INTO mustafa (minter, tokenid, pricepaid) VALUES (@minter, @tokenid, @pricepaid)";
                                //Console.WriteLine(minterObj.Minter);

                                using (var command = new NpgsqlCommand(sql, connection))
                                {
                                    // Parametre değerlerini belirleme
                                    command.Parameters.AddWithValue("minter", minterObj.Minter);
                                    command.Parameters.AddWithValue("tokenid", minterObj.TokenId);
                                    command.Parameters.AddWithValue("pricepaid", minterObj.PricePaid);

                                    // Veritabanına sorguyu gönderme
                                    command.ExecuteNonQuery();

                                    Console.WriteLine("Veri başarıyla eklendi.");
                                }
                            }
                        }

                    }
                    catch (OperationCanceledException)
                    {
                        // Kapatma işlemi başarılı bir şekilde gerçekleşirse
                    }
                    finally
                    {
                        consumer.Close();
                    }



                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Hata: {ex.Message}");
            }
        }
        
        


    }
}
public class MinterList
{

    public List<MinterObj> data { get; set; }
}
public class MinterObj
{
    public string Minter { get; set; }
    public string TokenId { get; set; }
    public string PricePaid { get; set; }
}