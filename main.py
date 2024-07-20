import pyaudio
from gtts import gTTS  
from playsound import playsound
import speech_recognition as sr
import time
import subprocess

def speak(answer):
    """Convert text to speech and play it."""
    tts = gTTS(answer.replace('•', '  *'))
    tts.save("audio.mp3")
    print(f"JARVIS: {answer}")
    playsound("audio.mp3")

def take_command():
    """Listen to user's speech and return the recognized text in lowercase."""
    r = sr.Recognizer()
    with sr.Microphone() as source:
        r.adjust_for_ambient_noise(source, duration=1)
        print("Say anything: ")
        audio = r.listen(source)
        try:
            query = r.recognize_google(audio, language='en-US').lower()
            print(f"User: {query}")
            return query.lower()
        except:
            query = "Sorry, could not recognize"
            print(query)
            return query
        
def main():
    Talk = True
    while Talk:
        user_said = take_command()
        if user_said == None :
            print("waiting") 
            continue
        if "hello" in user_said:
            speak("Hi")
        if "bye" in user_said:
            speak("goodbye")
        if "how are you" in user_said:
            speak("Doing well")
        if "stop" in user_said:
            speak("Stopping sir")
            break
        if "exit" in user_said:
            speak("ending program")
            Talk = False
        if "open my email" in user_said:
            speak("This is where I would run a program to open your email.")
        time.sleep(2)

main()

