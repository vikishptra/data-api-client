package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/semaphore"

	nikParser "data-api-client/nikparser"
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
		AllowOrigins:     "http://localhost:3000, https://localhost:3000, http://127.0.0.1:5500, https://127.0.0.1:5500, http://182.253.12.59:5500, http://ddns.sector.co.id:5500",
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
	app.Get("/search", func(c *fiber.Ctx) error {
		filterData := c.Query("filter")
		allResults := make(map[string]json.RawMessage)
		collections := []string{
			"sample_bansos", "bankraya_paylater", "bankraya_pinang_flexi", "data_posindo",
			"dekoruma_customer", "dekoruma_transaction", "dukcapil", "fithub",
			"gojek_customer", "portfolio_bnisekuritas", "rupa2_customer",
			"sicepat_customer", "sicepat_lama", "sim_pendaftaran", "sim_produksi",
			"tokopedia_cutomer", "user_posaja", "vehicle", "phone_regis",
		}

		var wg sync.WaitGroup
		sem := semaphore.NewWeighted(5)
		ctx, cancel := context.WithTimeout(c.Context(), 5*time.Second)
		defer cancel()

		for _, collectionName := range collections {
			wg.Add(1)
			go func(name string) {
				defer wg.Done()
				if err := sem.Acquire(ctx, 1); err != nil {
					fmt.Printf("Failed to acquire semaphore for %s: %v\n", name, err)
					return
				}
				defer sem.Release(1)

				result, err := fetchData(ctx, name, filterData)
				if err != nil {
					fmt.Printf("Error fetching data for %s: %v\n", name, err)
					return
				}

				if result != nil {
					allResults[name] = result
				}
			}(collectionName)
		}

		wg.Wait()

		return c.JSON(allResults)
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
			case "sample_bansos":
				filter = bson.D{
					{"$or", bson.A{
						bson.D{{"nama_lengkap", modifiedFilterData}},
						bson.D{{"nik", modifiedFilterData}},
					}},
				}
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

		if collectionName == "dukcapil" {
			fmt.Println("SASAS")
			processedData, err := processDukcapilData(cursor)
			if err != nil {
				return c.Status(500).SendString(err.Error())
			}
			return c.JSON(processedData)
		} else {
			if err = cursor.All(context.TODO(), &results); err != nil {
				return c.Status(500).SendString(err.Error())
			}
			return c.JSON(results)
		}
	})

	log.Fatal(app.Listen(":8000"))
}
func isAllDigits(s string) bool {
	for _, r := range s {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

func fetchData(ctx context.Context, collectionName, filterData string) (json.RawMessage, error) {
	apiURL := fmt.Sprintf("http://localhost:8000/detail-data?q=%s&filter=%s",
		url.QueryEscape(collectionName), url.QueryEscape(filterData))

	req, err := http.NewRequestWithContext(ctx, "GET", apiURL, nil)
	if err != nil {
		return nil, err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result json.RawMessage
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, err
	}

	if string(result) == "[]" || string(result) == "null" {
		return nil, nil
	}

	return result, nil
}

func ConvertReligion(religionInt int32) string {
	switch religionInt {
	case 1:
		return "ISLAM"
	case 2:
		return "KRISTEN"
	case 3:
		return "KATHOLIK"
	case 4:
		return "HINDU"
	case 5:
		return "BUDDHA"
	case 6:
		return "KHONGHUCU"
	default:
		return "-"
	}
}
func ConvertReligionString(religionStr string) (int32, string) {
	religionMap := map[string]int32{
		"ISLAM":     1,
		"KRISTEN":   2,
		"KATHOLIK":  3,
		"HINDU":     4,
		"BUDDHA":    5,
		"KHONGHUCU": 6,
	}

	religionInt, found := religionMap[religionStr]
	if found {
		return religionInt, ""
	}
	return 0, "data agama ini tidak di temukan, mohon di coba lagi!"
}

func ConvertRJenisKelaminString(jenisKelaminStr string) (int32, string) {
	jenisKelaminMap := map[string]int32{
		"PRIA":   1,
		"WANITA": 2,
	}

	jenisKelaminInt, found := jenisKelaminMap[jenisKelaminStr]
	if found {
		return jenisKelaminInt, ""
	}
	return 0, "data jenis kelamin ini tidak di temukan, mohon di coba lagi!"
}

func ConvertJenisKelamin(jenisKelaminInt int32) string {
	switch jenisKelaminInt {
	case 1:
		return "M"
	case 2:
		return "F"
	default:
		return "-"
	}
}

func ConvertBloodType(bloodTypeInt int32) string {
	switch bloodTypeInt {
	case 1:
		return "A"
	case 2:
		return "A+"
	case 3:
		return "A-"
	case 4:
		return "AB"
	case 5:
		return "AB+"
	case 6:
		return "AB-"
	case 7:
		return "B"
	case 8:
		return "B+"
	case 9:
		return "B-"
	case 10:
		return "O"
	case 11:
		return "O+"
	case 12:
		return "O-"
	default:
		return "-"
	}
}
func ConvertEducation(educationInt int32) string {
	switch educationInt {
	case 1:
		return "TIDAK / BELUM SEKOLAH"
	case 2:
		return "BELUM TAMAT SD / SEDERAJAT"
	case 3:
		return "TAMAT SD / SEDERAJAT"
	case 4:
		return "SLTP / SEDERAJAT"
	case 5:
		return "SLTA / SEDERAJAT"
	case 6:
		return "DIPLOMA I / II"
	case 7:
		return "AKADEMI / DIPLOMA III / SARJANA MUDA"
	case 8:
		return "DIPLOMA IV / STRATA I"
	case 9:
		return "STRATA II"
	case 10:
		return "STRATA III"
	default:
		return "-"
	}
}

func ConvertEducationString(educationStr string) (int32, string) {
	educationMap := map[string]int32{
		"TIDAK / BELUM SEKOLAH":                1,
		"BELUM TAMAT SD / SEDERAJAT":           2,
		"TAMAT SD / SEDERAJAT":                 3,
		"SLTP / SEDERAJAT":                     4,
		"SLTA / SEDERAJAT":                     5,
		"DIPLOMA I / II":                       6,
		"AKADEMI / DIPLOMA III / SARJANA MUDA": 7,
		"DIPLOMA IV / STRATA I":                8,
		"STRATA II":                            9,
		"STRATA III":                           10,
	}

	educationInt, found := educationMap[educationStr]
	if found {
		return educationInt, ""
	}

	return 0, "data edukasi ini tidak di temukan, mohon di coba lagi!"
}

func ConvertMaritalStatus(maritalStatusInt int32) string {
	switch maritalStatusInt {
	case 1:
		return "BELUM KAWIN"
	case 2:
		return "KAWIN"
	case 3:
		return "CERAI HIDUP"
	case 4:
		return "CERAI MATI"
	default:
		return "-"
	}
}
func ConvertMaritalStatusString(maritalStatusStr string) (int32, string) {
	maritalStatusMap := map[string]int32{
		"BELUM KAWIN": 1,
		"KAWIN":       2,
		"CERAI HIDUP": 3,
		"CERAI MATI":  4,
	}

	maritalStatusInt, found := maritalStatusMap[maritalStatusStr]
	if found {
		return maritalStatusInt, ""
	}

	return 0, "data marital status ini tidak di temukan, mohon di coba lagi!"
}

func ConvertOccupation(occupationInt int32) string {
	switch occupationInt {
	case 1:
		return "BELUM / TIDAK BEKERJA"
	case 2:
		return "MENGURUS RUMAH TANGGA"
	case 3:
		return "PELAJAR / MAHASISWA"
	case 4:
		return "PENSIUNAN"
	case 5:
		return "PEGAWAI NEGERI SIPIL"
	case 6:
		return "TNI"
	case 9:
		return "PETANI / PEKEBUN"
	case 10:
		return "PETERNAK"
	case 15:
		return "KARYAWAN SWASTA"
	case 19:
		return "BURUH HARIAN LEPAS"
	case 20:
		return "BURUH TANI / PERKEBUNAN"
	case 23:
		return "PEMBANTU RUMAH TANGGA"
	case 24:
		return "TUKANG CUKUR"
	case 25:
		return "TUKANG LISTRING"
	case 26:
		return "TUKANG BATU"
	case 27:
		return "TUKANG KAYU"
	case 34:
		return "MEKANIK"
	case 38:
		return "PARAJI"
	case 41:
		return "IMAM MASJID"
	case 44:
		return "WARTAWAN"
	case 45:
		return "USTADZ / MUBALIGH"
	case 65:
		return "GURU"
	case 81:
		return "SOPIR"
	case 84:
		return "PEDAGANG"
	case 85:
		return "PERANGKAT DESA"
	case 86:
		return "KEPALA DESA"
	case 87:
		return "BIARAWATI"
	case 88:
		return "WIRASWASTA"
	default:
		return "-"
	}

}

func ConvertStatusHubunganKeluarga(statusCode int32) string {
	switch statusCode {
	case 1:
		return "KEPALA KELUARGA"
	case 2:
		return "SUAMI"
	case 3:
		return "ISTRI"
	case 4:
		return "ANAK"
	case 5:
		return "MENANTU"
	case 6:
		return "CUCU"
	case 7:
		return "ORANGTUA"
	case 8:
		return "MERTUA"
	case 9:
		return "FAMILI LAIN"
	case 10:
		return "PEMBANTU"
	default:
		return "-"
	}
}

func ConvertToString(value interface{}) string {
	switch v := value.(type) {
	case string:
		return v
	case int32:
		return strconv.FormatInt(int64(v), 10)
	default:
		return "-"
	}
}

func ConvertToInt32(value interface{}) int32 {
	switch v := value.(type) {
	case int32:
		return v
	case int:
		return int32(v)
	case string:
		intValue, err := strconv.ParseInt(v, 10, 32)
		if err == nil {
			return int32(intValue)
		}
	}
	return 0
}

func ConvertToInt64(value interface{}) int64 {
	switch v := value.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case string:
		intValue, err := strconv.ParseInt(v, 10, 64)
		if err == nil {
			return intValue
		}
	}
	return 0
}
func processDukcapilData(cursor *mongo.Cursor) ([]map[string]interface{}, error) {
	var results []bson.M
	if err := cursor.All(context.TODO(), &results); err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return nil, nil
	}

	processedResults := make([]map[string]interface{}, len(results))

	for i, data := range results {
		nokkFirst := ConvertToInt64(data["NO_KK"])
		nikFirst := ConvertToInt64(data["NIK"])
		nikIbu := ConvertToInt64(data["NIK_IBU"])
		nikAyah := ConvertToInt64(data["NIK_AYAH"])
		namaLengkapFirst := ConvertToString(data["NAMA_LGKP"])
		jenisKelaminFirstInt := ConvertToInt32(data["JENIS_KLMIN"])
		tempatLahirFirst := ConvertToString(data["TMPT_LHR"])
		tanggalLahirFirst := ConvertToString(data["TGL_LHR"])
		bloodTypeFirstInt := ConvertToInt32(data["GOL_DRH"])
		educationFirstInt := ConvertToInt32(data["PDDK_AKH"])
		maritalStatusFirstInt := ConvertToInt32(data["STAT_KWN"])
		occupationFirstInt := ConvertToInt32(data["JENIS_PKRJN"])
		religionFirstInt := ConvertToInt32(data["AGAMA"])
		statuHubKeluargaFirstInt := ConvertToInt32(data["STAT_HBKEL"])
		noKelFirstInt := ConvertToString(data["NO_KEL"])

		religionFirst := ConvertReligion(religionFirstInt)
		jenisKelaminFirst := ConvertJenisKelamin(jenisKelaminFirstInt)
		bloodTypeFirst := ConvertBloodType(bloodTypeFirstInt)
		educationFirst := ConvertEducation(educationFirstInt)
		maritalStatusFirst := ConvertMaritalStatus(maritalStatusFirstInt)
		occupationFirst := ConvertOccupation(occupationFirstInt)
		statuHubKeluargaFirst := ConvertStatusHubunganKeluarga(statuHubKeluargaFirstInt)
		alamatFirst := nikParser.NikParser(nikFirst)

		processedResults[i] = map[string]interface{}{
			"NO_KK":          nokkFirst,
			"NIK":            nikFirst,
			"NAMA_LGKP":      namaLengkapFirst,
			"JENIS_KLMIN":    jenisKelaminFirst,
			"TMPT_LHR":       tempatLahirFirst,
			"TGL_LHR":        tanggalLahirFirst,
			"GOL_DRH":        bloodTypeFirst,
			"PDDK_AKH":       educationFirst,
			"STAT_KWN":       maritalStatusFirst,
			"JENIS_PKRJN":    occupationFirst,
			"AGAMA":          religionFirst,
			"STAT_HBKEL":     statuHubKeluargaFirst,
			"NO_KEL":         noKelFirstInt,
			"ALAMAT":         alamatFirst,
			"NIK_IBU":        nikIbu,
			"NAMA_LGKP_IBU":  data["NAMA_LGKP_IBU"],
			"NIK_AYAH":       nikAyah,
			"NAMA_LGKP_AYAH": data["NAMA_LGKP_AYAH"],
		}
	}

	return processedResults, nil
}
