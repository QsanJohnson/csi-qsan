package goqsan

import (
	"bytes"
	"crypto/aes"
)

func PKCS7Padding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padtext := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padtext...)
}

func PKCS7UnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}

func AESECBEncrypt(plaintext, key []byte) []byte {
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err.Error())
	}
	data := PKCS7Padding(plaintext, block.BlockSize())
	ciphertext := make([]byte, len(data))
	size := block.BlockSize()

	for bs, be := 0, size; bs < len(data); bs, be = bs+size, be+size {
		block.Encrypt(ciphertext[bs:be], data[bs:be])
	}

	return ciphertext
}

func AESECBDecrypt(ciphertext, key []byte) []byte {
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err.Error())
	}
	decrypted := make([]byte, len(ciphertext))
	size := block.BlockSize()

	for bs, be := 0, size; bs < len(ciphertext); bs, be = bs+size, be+size {
		block.Decrypt(decrypted[bs:be], ciphertext[bs:be])
	}

	plaintext := PKCS7UnPadding(decrypted)

	return plaintext
}

func contains(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}

	return false
}

func EqualStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
