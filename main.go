package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"strings"
	"unicode"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	mongoURI := os.Getenv("MONGODB_URI")

	app := fiber.New()
	app.Use(logger.New())
	app.Use(cors.New(cors.Config{
		AllowOrigins:     "http://localhost:3000, https://localhost:3000, http://127.0.0.1:5500, https://127.0.0.1:5500",
		AllowHeaders:     "Origin, Content-Type, Accept, Authorization,Set-Cookie",
		AllowMethods:     "GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS",
		AllowCredentials: true,
	}))
	clientOptions := options.Client().ApplyURI(mongoURI).SetMaxPoolSize(100)
	client, err := mongo.NewClient(clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	err = client.Connect(context.TODO())
	if err != nil {
		log.Fatal(err)

	}
	defer client.Disconnect(context.TODO())
	database := client.Database("sectorx")

	app.Get("/list-data", func(c *fiber.Ctx) error {
		collections, err := database.ListCollectionNames(context.TODO(), map[string]interface{}{})
		if err != nil {
			return c.Status(500).SendString(err.Error())
		}
		response := make(map[string]int64)

		for _, collectionName := range collections {
			collection := database.Collection(collectionName)
			count, err := collection.EstimatedDocumentCount(context.TODO())
			if err != nil {
				return c.Status(500).SendString(err.Error())
			}
			if count > 100 {
				response[collectionName] = count

			}
		}

		return c.JSON(response)
	})
	app.Get("/detail-data", func(c *fiber.Ctx) error {
		collectionName := c.Query("q")
		filterData := c.Query("filter")
		filterData = strings.ToUpper(filterData)

		if collectionName == "" {
			return c.Status(400).SendString("Query parameter 'q' is required")
		}

		collection := client.Database(database.Name()).Collection(collectionName)

		findOptions := options.Find()
		findOptions.SetLimit(10)
		var filter bson.D

		if filterData == "" {
			switch collectionName {
			case "bankraya_paylater":
				filter = bson.D{
					{"Hp", bson.D{{"$ne", -1}}},
					{"Tagihan Pokok", bson.D{{"$gt", 10000000}}},
				}
			case "bankraya_pinang_flexi":
				filter = bson.D{
					{"Tagihan Pokok", bson.D{{"$gt", 10000000}}},
				}
			case "rupa2_customer":
				filter = bson.D{
					{"email", bson.D{{"$ne", "-"}}},
				}
			case "user_posaja":
				filter = bson.D{
					{"Noidentitas", bson.D{
						{"$exists", true},
						{"$ne", ""},
						{"$ne", 0},
					}},
				}

			default:
				filter = bson.D{}
			}
		} else {
			var modifiedFilterData interface{}

			if isAllDigits(filterData) {
				nikInt, _ := strconv.Atoi(filterData)
				modifiedFilterData = nikInt
			} else {
				modifiedFilterData = filterData
			}
			switch collectionName {
			case "bankraya_paylater":
				filter = bson.D{
					{"$or", bson.A{
						bson.D{{"Nasabah", modifiedFilterData}},
						bson.D{{"Hp", modifiedFilterData}},
					}},
				}
			case "bankraya_pinang_flexi":
				filter = bson.D{
					{"$or", bson.A{
						bson.D{{"Nama Nasabah", modifiedFilterData}},
						bson.D{{"No Hp", modifiedFilterData}},
					}},
				}
			case "data_posindo":
				filter = bson.D{
					{"$or", bson.A{
						bson.D{{"nama_penerima", modifiedFilterData}},
						bson.D{{"nama_petugas", modifiedFilterData}},
						bson.D{{"nama_pengirim", modifiedFilterData}},
					}},
				}
			case "dekoruma_customer":
				filter = bson.D{
					{"$or", bson.A{
						bson.D{{"Email", modifiedFilterData}},
					}},
				}
			case "dekoruma_transaction":
				filter = bson.D{
					{"$or", bson.A{
						bson.D{{"Guest Email", modifiedFilterData}},
					}},
				}
			case "dukcapil":
				filter = bson.D{
					{"$or", bson.A{
						bson.D{{"NIK", modifiedFilterData}},
						bson.D{{"NAMA_LGKP", modifiedFilterData}},
					}},
				}
			case "fithub":
				filter = bson.D{
					{"$or", bson.A{
						bson.D{{"name", modifiedFilterData}},
						bson.D{{"email", modifiedFilterData}},
						bson.D{{"phone", filterData}},
					}},
				}
			case "gojek_customer":
				filter = bson.D{
					{"$or", bson.A{
						bson.D{{"Phone", modifiedFilterData}},
						bson.D{{"Email", modifiedFilterData}},
					}},
				}
			case "portfolio_bnisekuritas":
				filter = bson.D{
					{"$or", bson.A{
						bson.D{{"product.fundname", modifiedFilterData}},
						bson.D{{"cls_initialcode", modifiedFilterData}},
					}},
				}
			case "rupa2_customer":
				filter = bson.D{
					{"$or", bson.A{
						bson.D{{"customer_name", modifiedFilterData}},
						bson.D{{"phone", filterData}},
						bson.D{{"email", modifiedFilterData}},
					}},
				}
			case "sicepat_customer":
				filter = bson.D{
					{"$or", bson.A{
						bson.D{{"Consignee Name", modifiedFilterData}},
						bson.D{{"Consignee Phone", modifiedFilterData}},
						bson.D{{"Shipper Email", modifiedFilterData}},
						bson.D{{"Shipper Phone", modifiedFilterData}},
					}},
				}
			case "sicepat_lama":
				filter = bson.D{
					{"$or", bson.A{
						bson.D{{"Consignee Name", modifiedFilterData}},
						bson.D{{"Consignee Phone", modifiedFilterData}},
						bson.D{{"Shipper Email", modifiedFilterData}},
						bson.D{{"Shipper Phone", modifiedFilterData}},
					}},
				}
			case "sim_pendaftaran":
				filter = bson.D{
					{"$or", bson.A{
						bson.D{{"NAMA", modifiedFilterData}},
						bson.D{{"NIK", modifiedFilterData}},
					}},
				}
			case "sim_produksi":
				filter = bson.D{
					{"$or", bson.A{
						bson.D{{"NO SIM", modifiedFilterData}},
						bson.D{{"NAMA", modifiedFilterData}},
					}},
				}
			case "tokopedia_cutomer":
				filter = bson.D{
					{"$or", bson.A{
						bson.D{{"Full Name", modifiedFilterData}},
						bson.D{{"Telephone", modifiedFilterData}},
					}},
				}
			case "user_posaja":
				filter = bson.D{
					{"$or", bson.A{
						bson.D{{"Fullname", modifiedFilterData}},
						bson.D{{"Email", modifiedFilterData}},
						bson.D{{"Nophone", modifiedFilterData}},
						bson.D{{"Noidentitas", modifiedFilterData}},
					}},
				}
			case "vehicle":
				nameFilter := filterData + " "
				filter = bson.D{
					{"$or", bson.A{
						bson.D{{"NIK", modifiedFilterData}},
						bson.D{{"NAMA", nameFilter}},
						bson.D{{"NOPOL", modifiedFilterData}},
					}},
				}
			case "phone_regis":
				filter = bson.D{
					{"$or", bson.A{
						bson.D{{"NAME", modifiedFilterData}},
						bson.D{{"NIK", modifiedFilterData}},
						bson.D{{"PHONE", modifiedFilterData}},
					}},
				}
			default:
				filter = bson.D{}
			}
		}

		cursor, err := collection.Find(context.TODO(), filter, findOptions)
		if err != nil {
			return c.Status(500).SendString(err.Error())
		}
		defer cursor.Close(context.TODO())

		var results []bson.M
		if err = cursor.All(context.TODO(), &results); err != nil {
			return c.Status(500).SendString(err.Error())
		}

		return c.JSON(results)
	})

	log.Fatal(app.Listen(":8005"))
}
func isAllDigits(s string) bool {
	for _, r := range s {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}
