package main

import (
  "github.com/gocql/gocql"

  "log"
  "os"
  "net/url"
  "strings"
  "fmt"
)

func main() {
  // parse the URLs
  srcArr := strings.Split(os.Getenv("CASSANDRA_URL"), ",") 
  destArr := make([]string, 0, len(srcArr))

  var username string
  var password string

  for _, srcVal := range srcArr {
    the_url, err := url.Parse(srcVal)
    if err != nil {
      log.Fatal(err)
    }
    
    // these should be the same for all urls but whatev
    username = the_url.User.Username()
    password, _ = the_url.User.Password()
    destArr = append(destArr, the_url.Host)
  }

  // connect to the cluster
  cluster := gocql.NewCluster(destArr...)
  cluster.Authenticator = gocql.PasswordAuthenticator{
    Username: username,
    Password: password,
  }

  cluster.SslOpts = &gocql.SslOptions{
    CertPath:               "",
    KeyPath:                "",
    CaPath:                 "",
    EnableHostVerification: false,
  }

  fmt.Println("User:", username, password)

  cluster.Keyspace = "example"
  cluster.Consistency = gocql.Quorum
  session, _ := cluster.CreateSession()
  defer session.Close()

  // insert a tweet
  if err := session.Query(`INSERT INTO tweet (timeline, id, text) VALUES (?, ?, ?)`,
      "me", gocql.TimeUUID(), "hello world").Exec(); err != nil {
      log.Fatal(err)
  }

  var id gocql.UUID
  var text string

  /* Search for a specific set of records whose 'timeline' column matches
   * the value 'me'. The secondary index that we created earlier will be
   * used for optimizing the search */
  if err := session.Query(`SELECT id, text FROM tweet WHERE timeline = ? LIMIT 1`,
      "me").Consistency(gocql.One).Scan(&id, &text); err != nil {
      log.Fatal(err)
  }
  fmt.Println("Tweet:", id, text)

  // list all tweets
  iter := session.Query(`SELECT id, text FROM tweet WHERE timeline = ?`, "me").Iter()
  for iter.Scan(&id, &text) {
      fmt.Println("Tweet:", id, text)
  }
  if err := iter.Close(); err != nil {
      log.Fatal(err)
  }
}

