package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/RichardKnop/machinery/v2"
	redisbackend "github.com/RichardKnop/machinery/v2/backends/redis"
	redisbroker "github.com/RichardKnop/machinery/v2/brokers/redis"
	"github.com/RichardKnop/machinery/v2/config"
	eagerlock "github.com/RichardKnop/machinery/v2/locks/eager"
	"github.com/RichardKnop/machinery/v2/tasks"
)

const redisUrl = "localhost:3434"

func main() {

	var cnf = &config.Config{
		Broker:       "redis://" + redisUrl,
		DefaultQueue: "my_jobs",
		Redis: &config.RedisConfig{
			MaxIdle:                3,
			IdleTimeout:            240,
			ReadTimeout:            15,
			WriteTimeout:           15,
			ConnectTimeout:         15,
			NormalTasksPollPeriod:  1000,
			DelayedTasksPollPeriod: 500,
		},
	}

	broker := redisbroker.NewGR(cnf, []string{redisUrl}, 0)
	backend := redisbackend.NewGR(cnf, []string{redisUrl}, 0)
	lock := eagerlock.New()
	server := machinery.NewServer(cnf, broker, backend, lock)

	worker := server.NewWorker("test_worker", 10)
	errChan := make(chan error, 1)
	worker.LaunchAsync(errChan)

	server.RegisterTask("welcomeEmails", sendWelcomeEmail)
	server.RegisterTask("reminderEmails", sendWelcomeEmail)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT)

	// delayed job
	go func() {
		eta := time.Now().UTC().Add(time.Second * 15)
		signature := &tasks.Signature{
			Name: "reminderEmails",
			Args: []tasks.Arg{
				{
					Type:  "string",
					Value: "asma@gmail.com",
				},
				{
					Type:  "string",
					Value: "email:template:reminder",
				},
			},
			ETA: &eta,
		}
		reslt, _ := server.SendTask(signature)
		data, _ := reslt.Get(0)
		fmt.Println("\n Resultttttt: ", data[0])
	}()

	ticker := time.NewTicker(time.Second * 5)
	go func() {
		for {
			select {
			case <-errChan:
				return
			case <-ticker.C:
				signature := &tasks.Signature{
					Name: "welcomeEmails",
					Args: []tasks.Arg{
						{
							Type:  "string",
							Value: "wassim@gmail.com",
						},
						{
							Type:  "string",
							Value: "email:template:welcome",
						},
					},
				}

				reslt, _ := server.SendTask(signature)
				data, _ := reslt.Get(0)
				fmt.Println("\n Resultttttt: ", data[0])

			}
		}
	}()

	<-sigChan
	//
}
func sendWelcomeEmail(to string, template_id string) (string, error) {
	fmt.Println("sending email...")

	time.Sleep(time.Second * 3)

	return "sent to " + to, nil
}
