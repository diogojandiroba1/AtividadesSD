
tamanho_mb = [5, 50, 500]
index = 0;
def gerar_arq(tamanho_mb: int):
    tamanho_bytes = tamanho_mb * 1024 * 1024
    with open(f'arquivo_lixo_{index}.bin', "wb") as arquivo:
        index+=1
        # Escreve bytes aleatórios de uma vez só
        arquivo.write(bytearray(tamanho_bytes))

    print("Arquivo lixo criado com sucesso!")


if __name__ == '__main__':
    for i in tamanho_mb:
        gerar_arq(i)