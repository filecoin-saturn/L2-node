# Running a Saturn L2 node

1. cd into saturn-l2.
2. Run `go build -o saturn-l2-svc main/main.go`. This builds a binary called `saturn-l2-svc` in your current directory.
3. You can start the binary by giving it the port to bind to:
     ```
   aarshshah@192 saturn-l2 % ./saturn-l2-svc 1234                  
   Server listening on [::]:1234
     ```
4. It has an http endpoint called `hello` that returns some JSON:   
```
aarshshah@192 ~ % curl http://127.0.0.1:1234/hello
  {"Age":25,"Name":"BajtosTheGreat"}
 ```

