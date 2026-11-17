namespace Models;

public class Item
{
    public required string NomeDoProduto { get; set; }
    public required int Quantidade { get; set; }
    public required decimal PrecoUnitario { get; set; }
}