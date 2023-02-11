const fs = require('fs')
const readline = require('readline')
const path = require('path')

const MAX_MEMORY = 500 * 1024 * 1024 // Максимальный объем памяти, доступный для сортировки
const CHUNK_SIZE = 100 * 1024 * 1024 // Размер чанка, используемый для чтения и записи
const INPUT_FILE = 'file.txt'
const OUTPUT_FILE = 'sorted_output.txt'
const TEMP_FOLDER = 'temp'

sortFile(INPUT_FILE)
async function sortFile(INPUT_FILE) {
    const fileStats = fs.statSync(INPUT_FILE)
    const maxChanks = fileStats.size / CHUNK_SIZE // Определяем количество итераций
    const iterator = readChunks(INPUT_FILE)
    console.log('Сортировка запущена...')
    const f = async i => {
        if (i === Math.ceil(maxChanks)) {
            await fixString()
        } else {
            try {
                await iterator.next().then(function (result) {
                    sortAndWriteChunk(result.value.chunk, result.value.chunk_index) //Вызываем функцию по созданию и сортировке маленьких файлов для каждой итерации
                })
            } catch (err) {
                // console.log(err)
            }
            f(i + 1)
        }
    }
    f(0)

    await mergeChunks() //Объединяем все полученные файлы
}

//Собираем строки для маленьких файлов
async function* readChunks(INPUT_FILE) {
    const fileStream = fs.createReadStream(INPUT_FILE)
    const rl = readline.createInterface({
        input: fileStream
    })
    let chunk = ''
    let chunk_index = -1
    for await (const line of rl) {
        chunk += `${line}\n`
        if (chunk.length > CHUNK_SIZE) {
            chunk_index++
            yield { chunk, chunk_index }
            chunk = ''
        }
    }
    if (chunk) {
        chunk_index++
        yield { chunk, chunk_index }
    }
}

//Создаем и сортируем маленькие файлы
async function sortAndWriteChunk(chunk, chunkIndex) {
    const lines = chunk.split('\n').filter(line => line.length > 0)
    lines.sort()
    const chunkPath = path.join(TEMP_FOLDER, `chunk_${chunkIndex}.txt`)
    await fs.promises.writeFile(`./${TEMP_FOLDER}/chunk_${chunkIndex}.txt`, lines.join('\n'))
}


//Объединяем и сортируем маленькие файлы
async function mergeChunks() {
    try {
        let chunkIndex = 0
        let outputFilePath = path.join(__dirname, OUTPUT_FILE)
        while (chunkIndex === 0 || fs.existsSync(path.join(TEMP_FOLDER, `chunk_${chunkIndex}.txt`))) {//Пробегаемся по всем временным файлам
            const chunkPaths = []
            let totalLength = 0
            while (totalLength < MAX_MEMORY) {//Собираем файлы, котоыре мы можем объединить за один раз
                const chunkPath = path.join(TEMP_FOLDER, `chunk_${chunkIndex}.txt`)
                if (!fs.existsSync(chunkPath)) {
                    break
                }
                const stat = fs.statSync(chunkPath)
                totalLength += stat.size
                chunkPaths.push(chunkPath)
                chunkIndex++
            }
            //Запускаем стриминг, чтобы записать временные файлы в один файл
            const chunkStreams = chunkPaths.map(chunkPath => fs.createReadStream(chunkPath))
            const mergedStream = require('merge-stream')(chunkStreams)
            const outputStream = fs.createWriteStream(outputFilePath, { flags: 'a' })
            const rl = readline.createInterface({
                input: mergedStream
            })
            const lines = []
            for await (const line of rl) {
                lines.push(line)
                if (lines.length >= MAX_MEMORY) {
                    lines.sort()
                    outputStream.write(`${lines.join('\n')}\n`)
                    lines.length = 0
                }
            }
            if (lines.length > 0) {
                lines.sort()
                outputStream.write(`${lines.join('\n')}\n`)
            }
            
            //Удаляем временные файлы
            chunkPaths.forEach(chunkPath => { 
                fs.unlinkSync(chunkPath) 
            })
        }
    } catch (err) {
        console.log(err)
    }
}

async function fixString() {

    let chunkIndex = 0
    while (chunkIndex === 0 || fs.existsSync(path.join(TEMP_FOLDER, `chunk_${chunkIndex}.txt`))) {//Пробегаемся по всем временным файлам
        const chunkPaths = []
        let totalLength = 0
        while (totalLength < MAX_MEMORY) {//Собираем файлы, котоыре мы можем объединить за один раз
            const chunkPath = path.join(TEMP_FOLDER, `chunk_${chunkIndex}.txt`)
            if (!fs.existsSync(chunkPath)) {
                break
            }
            const stat = fs.statSync(chunkPath)
            totalLength += stat.size
            chunkPaths.push(chunkPath)
            chunkIndex++
        }
        await chunkPaths.forEach((file) => {
            fs.readFile(file, 'utf8', (err, data) => {
                if (err) throw err
                const updatedData = data + '\n'
                fs.writeFile(file, updatedData, (err) => {
                    if (err) throw err
                })
            })
        })
    }
}